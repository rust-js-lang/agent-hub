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
fn a_message_to_a_channel_reaches_each_agent_in_it_but_its_sender() {
    let (client, key) = hub_with_key();
    for name in ["claude", "codex", "gemini"] {
        client.register(name).unwrap();
    }
    client.sign_in("tu", &key).unwrap();
    let to = |sent: &[protocol::Message]| sent.iter().map(|m| m.to.clone()).collect::<Vec<_>>();

    // Every agent is in `everyone`; the person isn't, nor the sender sent a copy.
    let sent = client
        .broadcast("claude", "#everyone", "Rebasing main.", None)
        .unwrap();
    assert_eq!(to(&sent), ["codex", "gemini"]);
    assert!(
        sent.iter()
            .all(|m| m.channel.as_deref() == Some("everyone") && !m.person)
    );
    let inbox = client.read_inbox("codex", 20).unwrap();
    assert_eq!(inbox.messages[0].channel.as_deref(), Some("everyone"));

    // A channel is who joined it; the person's message to it says so.
    let team = client.join("#team", "codex").unwrap();
    assert_eq!((team.name.as_str(), team.members.len()), ("team", 1));
    client.join("team", "gemini").unwrap();
    let names =
        |channels: Vec<protocol::Channel>| channels.into_iter().map(|c| c.name).collect::<Vec<_>>();
    assert_eq!(names(client.channels().unwrap()), ["everyone", "team"]);
    let sent = client
        .broadcast("tu", "team", "Ship it.", Some(&key))
        .unwrap();
    assert_eq!(to(&sent), ["codex", "gemini"]);
    assert!(sent.iter().all(|m| m.person));
    assert_eq!(client.leave("team", "gemini").unwrap().members, ["codex"]);
    assert_eq!(
        to(&client
            .broadcast("tu", "team", "Again.", Some(&key))
            .unwrap()),
        ["codex"]
    );

    let refused = |result: anyhow::Result<Vec<protocol::Message>>| result.unwrap_err().to_string();
    assert_eq!(
        refused(client.broadcast("codex", "team", "Only me here.", None)),
        "no other agent is in #team: `cross-agent-message join team` puts one in"
    );
    assert_eq!(
        refused(client.broadcast("tu", "team", "Without the key.", None)),
        "tu is the person's name on this hub: only the dashboard, with the hub's key, sends as them"
    );
    assert_eq!(
        client.join("everyone", "codex").unwrap_err().to_string(),
        "every agent is in #everyone: it's neither joined nor left"
    );
    assert_eq!(
        client.join("team", "tu").unwrap_err().to_string(),
        "tu is the person, who reads every channel on the dashboard"
    );
}

/// A hub and its key, as `cross-agent-message key` reads it of its file.
fn hub_with_key() -> (Client, String) {
    let store = Store::in_memory().unwrap();
    let key = store.person_key().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let address = listener.local_addr().unwrap();
    let app = hub::router(
        Arc::new(Hub::new(store).unwrap()),
        PathBuf::from("no-dashboard"),
    );
    thread::spawn(move || runtime.block_on(async { axum::serve(listener, app).await.unwrap() }));
    (Client::new(&format!("http://{address}")), key)
}

#[test]
fn the_person_writes_from_the_dashboard_with_the_hubs_key() {
    let (client, key) = hub_with_key();
    client.register("claude").unwrap();
    let refused = |result: anyhow::Result<protocol::Agent>| result.unwrap_err().to_string();

    // Signing in takes the hub's key, and a name no agent has.
    assert_eq!(
        refused(client.sign_in("tu", "0000-0000-0000")),
        "that isn't this hub's key: `cross-agent-message key`, on the hub's machine, prints it"
    );
    assert_eq!(
        refused(client.sign_in("claude", &key)),
        "claude is an agent's name on this hub: pick another"
    );
    // By its digits alone, as a phone's number keys type it: no dash.
    let digits: String = key.chars().filter(char::is_ascii_digit).collect();
    assert_eq!(digits.len(), 12);
    assert!(client.sign_in("tu", &digits).unwrap().person);
    assert_eq!(
        refused(client.sign_in("tu", "---")),
        "that isn't this hub's key: `cross-agent-message key`, on the hub's machine, prints it"
    );
    let tu = client.sign_in("tu", &key).unwrap();
    assert!(tu.person);
    assert_eq!(tu.machine, "dashboard");

    // Their message says it's theirs; an agent's never does.
    let sent = client
        .send_as_person("tu", "claude", "Ship it.", &key)
        .unwrap();
    assert!(sent.person);
    let inbox = client.read_inbox("claude", 20).unwrap();
    assert!(inbox.messages[0].person);
    let back = client.send("claude", "tu", "Shipping.").unwrap();
    assert!(!back.person);

    // Nothing else is sent as the person, and no agent takes their name.
    let refused = |result: anyhow::Result<protocol::Message>| result.unwrap_err().to_string();
    assert_eq!(
        refused(client.send("tu", "claude", "As them, without the key.")),
        "tu is the person's name on this hub: only the dashboard, with the hub's key, sends as them"
    );
    assert_eq!(
        refused(client.send_as_person("tu", "claude", "A wrong key.", "1111-1111-1111")),
        "that isn't this hub's key: `cross-agent-message key`, on the hub's machine, prints it"
    );
    assert_eq!(
        refused(client.send_as_person("claude", "tu", "An agent with the key.", &key)),
        "claude isn't the person's name: sign in from the dashboard first"
    );
    assert_eq!(
        client.register("tu").unwrap_err().to_string(),
        "tu is the person's name on this hub, who signs in from the dashboard: pick another"
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
