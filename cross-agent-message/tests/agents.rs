//! Agents talking through a real hub, over HTTP, as the command line does.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cross_agent_message::client::Client;
use cross_agent_message::hub::{self, Hub};
use cross_agent_message::store::Store;
use protocol::Event;

/// A hub on a free port, with an empty store, serving `dashboard`.
fn start(dashboard: PathBuf) -> Client {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let address = listener.local_addr().unwrap();
    let hub = Hub::new(Store::in_memory().unwrap()).unwrap();
    let app = hub::router(Arc::new(hub), dashboard);
    thread::spawn(move || runtime.block_on(async { axum::serve(listener, app).await.unwrap() }));
    Client::new(&format!("http://{address}"))
}

fn hub() -> Client {
    start(PathBuf::from("no-dashboard"))
}

#[test]
fn a_message_waits_in_the_inbox_until_read() {
    let client = hub();
    client.register("codex").unwrap();
    client.register("claude").unwrap();

    let sent = client
        .send("codex", "claude", "I took the parser.")
        .unwrap();
    assert_eq!((sent.from.as_str(), sent.to.as_str()), ("codex", "claude"));
    assert_eq!(sent.read_at, None);
    let claude = client
        .agents()
        .unwrap()
        .into_iter()
        .find(|a| a.name == "claude")
        .unwrap();
    assert_eq!(claude.unread, 1);

    let inbox = client.read_inbox("claude", 20).unwrap();
    assert_eq!(inbox.remaining, 0);
    assert_eq!(inbox.messages.len(), 1);
    assert_eq!(inbox.messages[0].body, "I took the parser.");
    assert!(inbox.messages[0].read_at.is_some());
    assert!(client.read_inbox("claude", 20).unwrap().messages.is_empty());
    assert!(client.read_inbox("codex", 20).unwrap().messages.is_empty());
}

#[test]
fn the_hub_refuses_what_it_cant_deliver() {
    let client = hub();
    client.register("codex").unwrap();

    let refused = |result: anyhow::Result<protocol::Message>| result.unwrap_err().to_string();
    assert_eq!(
        refused(client.send("codex", "nobody", "hi")),
        "no agent is named nobody: `cross-agent-message agents` lists them"
    );
    assert_eq!(
        refused(client.send("nobody", "codex", "hi")),
        "no agent is named nobody: `cross-agent-message agents` lists them"
    );
    assert_eq!(
        refused(client.send("codex", "codex", "hi")),
        "codex can't send a message to itself"
    );
    client.register("claude").unwrap();
    assert_eq!(
        refused(client.send("codex", "claude", "  ")),
        "a message can't be empty"
    );
    assert!(
        client
            .register("has space")
            .unwrap_err()
            .to_string()
            .contains("letters, digits")
    );
}

#[test]
fn a_watch_hears_its_own_messages_and_marks_it_watching() {
    let client = hub();
    client.register("codex").unwrap();
    client.register("claude").unwrap();
    client.register("gemini").unwrap();

    let mut events = client.watch("claude").unwrap();
    let watching = |name: &str| {
        client
            .agents()
            .unwrap()
            .into_iter()
            .find(|a| a.name == name)
            .unwrap()
            .watching
    };
    assert!(watching("claude"));
    assert!(!watching("codex"));

    client.send("codex", "gemini", "not for claude").unwrap();
    client
        .send("codex", "claude", "for claude\nsecond line")
        .unwrap();
    match events.next().unwrap().unwrap() {
        Event::Sent { message } => {
            assert_eq!(message.to, "claude");
            assert_eq!(message.body, "for claude\nsecond line");
        }
        other => panic!("a watch hears only messages to it, and heard {other:?}"),
    }

    drop(events);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while watching("claude") {
        assert!(
            std::time::Instant::now() < deadline,
            "claude still watching after its stream closed"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_hold_is_one_agents_at_a_time() {
    let client = hub();
    client.register("codex").unwrap();
    client.register("claude").unwrap();
    assert!(client.holds().unwrap().is_empty());

    let taken = client.take("check", "codex", 30).unwrap();
    assert!(taken.yours);
    assert_eq!(taken.hold.holder, "codex");
    let other = client.take("check", "claude", 30).unwrap();
    assert!(!other.yours);
    assert_eq!(other.hold, taken.hold);
    assert_eq!(client.holds().unwrap(), std::slice::from_ref(&taken.hold));

    assert_eq!(
        client.release("check", "claude").unwrap_err().to_string(),
        "codex holds check, not claude: only its holder lets it go"
    );
    assert!(client.release("check", "codex").unwrap().is_empty());
    assert!(client.take("check", "claude", 30).unwrap().yours);

    let refused = |result: anyhow::Result<protocol::Taken>| result.unwrap_err().to_string();
    assert_eq!(
        refused(client.take("check", "nobody", 30)),
        "no agent is named nobody: `cross-agent-message agents` lists them"
    );
    assert_eq!(
        refused(client.take("all cores", "codex", 30)),
        "a hold's name is letters, digits, `-`, `_` and `.`, and \"all cores\" has ' '"
    );
    assert_eq!(
        refused(client.take("check", "codex", 0)),
        "a hold is taken for 1 to 240 minutes, not 0: take it again to keep it longer"
    );
}

#[test]
fn the_hub_says_which_build_it_is() {
    let client = hub();
    let info = client.info().unwrap();
    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.build, hub::BUILD);
    assert!(info.started > 0);
    // Asked to stop, it answers with what it was first.
    assert_eq!(client.stop().unwrap(), info);
}

#[test]
fn history_shows_who_sent_what_to_whom() {
    let client = hub();
    for name in ["a", "b", "c"] {
        client.register(name).unwrap();
    }
    client.send("a", "b", "1").unwrap();
    client.send("b", "c", "2").unwrap();
    let history: Vec<(String, String, String)> =
        ureq::get(format!("{}{}", base(&client), protocol::MESSAGES))
            .call()
            .unwrap()
            .body_mut()
            .read_json::<Vec<protocol::Message>>()
            .unwrap()
            .into_iter()
            .map(|m| (m.from, m.to, m.body))
            .collect();
    let row =
        |from: &str, to: &str, body: &str| (from.to_string(), to.to_string(), body.to_string());
    assert_eq!(history, [row("b", "c", "2"), row("a", "b", "1")]);
}

#[test]
fn the_dashboard_is_served_beside_the_api() {
    let dir = std::env::temp_dir().join(format!("cross-agent-message-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("index.html"), "<p>dashboard</p>").unwrap();
    let client = start(dir.clone());
    let page = ureq::get(format!("{}/", base(&client)))
        .call()
        .unwrap()
        .body_mut()
        .read_to_string()
        .unwrap();
    assert_eq!(page, "<p>dashboard</p>");
    let missing = ureq::get(format!("{}/missing.js", base(&client))).call();
    assert!(matches!(missing, Err(ureq::Error::StatusCode(404))));
    std::fs::remove_dir_all(dir).unwrap();
}

/// The hub's address, as the client was made with.
fn base(client: &Client) -> String {
    client.hub().to_string()
}
