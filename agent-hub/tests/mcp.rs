//! Real MCP stdio clients and native clients exchanging mail through one HTTP hub.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agent_hub::client::Client;
use agent_hub::hub::{self, Hub};
use agent_hub::store::Store;
use axum::{Json, Router, routing::get};
use protocol::{Inbox, Message, ReadInbox};
use rmcp::model::{CallToolRequestParams, CallToolResult, ProtocolVersion};
use rmcp::service::{RoleClient, RunningService};
use rmcp::{ClientLifecycleMode, ClientServiceExt};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;

struct TestHub {
    url: String,
    task: JoinHandle<()>,
}

impl TestHub {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let hub = Hub::new(Store::in_memory().unwrap()).unwrap();
        let app = hub::router(Arc::new(hub), PathBuf::from("no-dashboard"));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, task }
    }

    async fn native<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Client) -> T + Send + 'static,
    ) -> T {
        let url = self.url.clone();
        tokio::task::spawn_blocking(move || operation(Client::new(&url)))
            .await
            .unwrap()
    }
}

impl Drop for TestHub {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Bridge {
    peer: RunningService<RoleClient, ()>,
    process: Child,
}

impl Bridge {
    async fn start(hub: &TestHub, name: &str, modern: bool) -> Self {
        let mut process = Command::new(env!("CARGO_BIN_EXE_agent-hub"))
            .args(["mcp", "--as", name, "--hub", &hub.url])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let transport = (
            process.stdout.take().unwrap(),
            process.stdin.take().unwrap(),
        );
        let lifecycle = if modern {
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            }
        } else {
            ClientLifecycleMode::Initialize
        };
        let peer = tokio::time::timeout(
            Duration::from_secs(10),
            ().serve_with_lifecycle(transport, lifecycle),
        )
        .await
        .unwrap()
        .unwrap();
        Self { peer, process }
    }

    async fn call(&self, name: &str, args: Value) -> CallToolResult {
        self.peer
            .call_tool(
                CallToolRequestParams::new(name.to_string())
                    .with_arguments(args.as_object().unwrap().clone()),
            )
            .await
            .unwrap()
    }

    async fn close(mut self) {
        self.peer.cancel().await.unwrap();
        let status = tokio::time::timeout(Duration::from_secs(5), self.process.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
    }
}

fn value<T: DeserializeOwned>(result: CallToolResult) -> T {
    assert_ne!(result.is_error, Some(true), "{result:?}");
    serde_json::from_value(result.structured_content.unwrap()).unwrap()
}

fn peek(name: &str) -> ReadInbox {
    ReadInbox {
        name: name.to_string(),
        max: 20,
        channel: None,
        sender: None,
        skip: false,
        digest: false,
        defer_ack: true,
        up_to: None,
    }
}

async fn exchange(modern: bool) {
    let hub = TestHub::start().await;
    hub.native(|client| client.register("claude").unwrap())
        .await;
    let bridge = Bridge::start(&hub, "codex", modern).await;
    let tools = bridge.peer.list_all_tools().await.unwrap();
    let mut names: Vec<_> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "ack_messages",
            "list_agents",
            "read_inbox",
            "read_message",
            "send_message"
        ]
    );

    let agents: Vec<protocol::Agent> = value(bridge.call("list_agents", json!({})).await);
    assert!(agents.iter().any(|agent| agent.name == "codex"));
    let sent: Vec<Message> = value(
        bridge
            .call(
                "send_message",
                json!({
                    "to": "claude", "body": "Please check the parser."
                }),
            )
            .await,
    );
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].from, "codex");
    let native_inbox = hub
        .native(|client| client.read_inbox("claude", 20).unwrap())
        .await;
    assert_eq!(native_inbox.messages[0].id, sent[0].id);

    let reply = hub
        .native(|client| {
            client
                .send("claude", "codex", "The parser looks good.")
                .unwrap()
        })
        .await;
    // Repeated reads model a lost response: neither one acknowledges delivery.
    for _ in 0..2 {
        let inbox: Inbox = value(bridge.call("read_inbox", json!({})).await);
        assert_eq!(inbox.messages, vec![reply.clone()]);
        assert_eq!(inbox.messages[0].read_at, None);
    }
    let read: Message = value(bridge.call("read_message", json!({"id": reply.id})).await);
    assert_eq!(read, reply);
    let unread = hub
        .native(|client| client.read_mail(&peek("codex")).unwrap())
        .await;
    assert_eq!(unread.messages.len(), 1);
    let acked: Vec<u64> = value(
        bridge
            .call("ack_messages", json!({"ids": [reply.id]}))
            .await,
    );
    assert_eq!(acked, [reply.id]);
    let acked_again: Vec<u64> = value(
        bridge
            .call("ack_messages", json!({"ids": [reply.id]}))
            .await,
    );
    assert!(acked_again.is_empty());
    assert!(
        hub.native(|client| client.read_mail(&peek("codex")).unwrap())
            .await
            .messages
            .is_empty()
    );
    bridge.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn modern_mcp_and_cli_share_mail_with_explicit_acknowledgment() {
    exchange(true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_mcp_and_cli_share_mail_with_explicit_acknowledgment() {
    exchange(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn two_stdio_bridges_exchange_messages_through_one_hub() {
    let hub = TestHub::start().await;
    let codex = Bridge::start(&hub, "codex", true).await;
    let claude = Bridge::start(&hub, "claude", false).await;
    let sent: Vec<Message> = value(
        codex
            .call(
                "send_message",
                json!({
                    "to": "claude", "body": "Please review the change."
                }),
            )
            .await,
    );
    let inbox: Inbox = value(claude.call("read_inbox", json!({})).await);
    assert_eq!(inbox.messages, sent);
    let _: Vec<u64> = value(
        claude
            .call("ack_messages", json!({"ids": inbox.read_ids}))
            .await,
    );
    let replied: Vec<Message> = value(
        claude
            .call(
                "send_message",
                json!({
                    "to": "codex", "body": "Review complete."
                }),
            )
            .await,
    );
    let inbox: Inbox = value(codex.call("read_inbox", json!({})).await);
    assert_eq!(inbox.messages, replied);
    codex.close().await;
    claude.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bridge_identity_and_recipient_scope_cannot_be_changed_by_tool_arguments() {
    let hub = TestHub::start().await;
    let foreign = hub
        .native(|client| {
            client.register("claude").unwrap();
            client.register("other").unwrap();
            client.send("claude", "other", "Private to other.").unwrap()
        })
        .await;
    let bridge = Bridge::start(&hub, "codex", true).await;
    let spoof = bridge
        .peer
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                json!({"from": "claude", "to": "other", "body": "Spoofed."})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await;
    assert!(
        match spoof {
            Ok(result) => result.is_error == Some(true),
            Err(_) => true,
        },
        "unknown sender argument must be rejected"
    );
    assert_eq!(
        bridge
            .call("read_message", json!({"id": foreign.id}))
            .await
            .is_error,
        Some(true)
    );
    let acked: Vec<u64> = value(
        bridge
            .call("ack_messages", json!({"ids": [foreign.id]}))
            .await,
    );
    assert!(acked.is_empty());
    let other = hub
        .native(|client| client.read_mail(&peek("other")).unwrap())
        .await;
    assert_eq!(other.messages, [foreign]);
    for max in [0, 101] {
        assert_eq!(
            bridge
                .call("read_inbox", json!({"max": max}))
                .await
                .is_error,
            Some(true)
        );
    }
    assert_eq!(
        bridge
            .call("send_message", json!({"to": "unknown", "body": "Hello"}))
            .await
            .is_error,
        Some(true)
    );
    assert_eq!(
        bridge
            .call("send_message", json!({"to": "claude", "body": " "}))
            .await
            .is_error,
        Some(true)
    );
    bridge.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn channel_mail_uses_existing_fanout_and_retirement_stops_a_connected_bridge() {
    let hub = TestHub::start().await;
    hub.native(|client| {
        client.register("claude").unwrap();
        client.join("reviewers", "claude").unwrap();
    })
    .await;
    let bridge = Bridge::start(&hub, "codex", true).await;
    let sent: Vec<Message> = value(
        bridge
            .call(
                "send_message",
                json!({
                    "to": "#reviewers", "body": "Review is ready."
                }),
            )
            .await,
    );
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].channel.as_deref(), Some("reviewers"));
    let direct = hub
        .native(|client| {
            client.join("reviewers", "codex").unwrap();
            client
                .broadcast("claude", "reviewers", "Channel reply.", None)
                .unwrap();
            client.send("claude", "codex", "Direct reply.").unwrap()
        })
        .await;
    let channel: Inbox = value(
        bridge
            .call("read_inbox", json!({"channel": "#reviewers"}))
            .await,
    );
    assert_eq!(channel.messages.len(), 1);
    assert_eq!(channel.messages[0].body, "Channel reply.");
    assert_ne!(channel.messages[0].id, direct.id);
    hub.native(|client| client.retire("codex").unwrap()).await;
    assert_eq!(
        bridge.call("list_agents", json!({})).await.is_error,
        Some(true)
    );
    assert!(
        agent_hub::mcp::Messaging::connect(&hub.url, "codex")
            .await
            .is_err()
    );
    bridge.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn old_hubs_are_refused_before_registration_or_any_mail_operation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fallback_calls = calls.clone();
    let mut info = Hub::new(Store::in_memory().unwrap()).unwrap().info();
    info.features.retain(|feature| feature != "read-ack");
    let app = Router::new()
        .route("/api/hub", get(move || async move { Json(info) }))
        .fallback(move || {
            fallback_calls.fetch_add(1, Ordering::SeqCst);
            async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let error = agent_hub::mcp::Messaging::connect(&url, "codex")
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("read-ack"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    task.abort();
}
