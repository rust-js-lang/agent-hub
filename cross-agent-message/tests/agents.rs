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

#[test]
fn retirement_ends_a_watch_and_reconnect_does_not_revive_the_name() {
    let client = hub();
    client.register("a").unwrap();
    client.register("b").unwrap();
    let old = client.send("a", "b", "old life").unwrap();
    client.take("check", "b", 30).unwrap();
    client.join("team", "b").unwrap();
    let mut events = client.watch("b").unwrap();
    let retired = client.retire("b").unwrap();
    assert_eq!((retired.channels, retired.holds), (1, 1));
    assert!(matches!(events.next().unwrap().unwrap(), Event::Retired { name } if name == "b"));
    assert!(events.next().is_none());
    assert!(client.holds().unwrap().is_empty());
    assert!(client.agents().unwrap().iter().all(|a| a.name != "b"));
    let error = client.register_connection("b", true).unwrap_err();
    assert!(
        error
            .downcast_ref::<cross_agent_message::client::RetiredAgent>()
            .is_some()
    );
    assert!(client.send("a", "b", "still retired").is_err());
    assert!(client.agents().unwrap().iter().all(|a| a.name != "b"));
    client.register("b").unwrap();
    assert_eq!(client.read_inbox("b", 20).unwrap().messages.len(), 0);
    assert!(client.read_message("b", old.id).is_err());
    let new = client.send("a", "b", "new life").unwrap();
    assert_eq!(client.read_message("b", new.id).unwrap().body, "new life");
    assert!(client.read_message("a", new.id).is_err());
}

#[test]
fn http_digest_and_notice_do_not_read_mail_and_human_broadcasts_keep_priority() {
    let (client, key) = hub_with_key();
    client.register("a").unwrap();
    client.register("b").unwrap();
    client.sign_in("tu", &key).unwrap();
    client.join("team", "b").unwrap();
    let ordinary = client
        .broadcast("a", "team", "short ask\nlong body", None)
        .unwrap()[0]
        .id;
    let human = client
        .broadcast("tu", "team", "urgent human ask", Some(&key))
        .unwrap()[0]
        .id;
    let counts = client.notice("b", false).unwrap();
    assert_eq!(counts, [("tu".to_string(), true, 1)]);
    assert_eq!(
        client
            .notice("b", true)
            .unwrap()
            .iter()
            .map(|(_, _, n)| n)
            .sum::<u32>(),
        2
    );
    assert_eq!(
        client
            .agents()
            .unwrap()
            .iter()
            .find(|a| a.name == "b")
            .unwrap()
            .unread,
        2
    );
    let request = protocol::ReadInbox {
        name: "b".to_string(),
        max: 20,
        channel: None,
        sender: None,
        skip: false,
        digest: true,

        defer_ack: false,

        up_to: None,
    };
    let digest = client.read_mail(&request).unwrap();
    assert_eq!(
        digest.messages.iter().map(|m| m.id).collect::<Vec<_>>(),
        [human]
    );
    assert_eq!(digest.digests[0].first[0].id, ordinary);
    assert_eq!(
        client
            .agents()
            .unwrap()
            .iter()
            .find(|a| a.name == "b")
            .unwrap()
            .unread,
        1
    );
    let repeat = client.read_mail(&request).unwrap();
    assert_eq!(repeat.digests[0].unread, 1);
    assert!(repeat.messages.is_empty());
    assert!(client.notice("b", false).unwrap().is_empty());
    assert_eq!(
        client.read_message("b", ordinary).unwrap().body,
        "short ask\nlong body"
    );
    assert!(client.read_mail(&request).unwrap().digests.is_empty());
}

#[test]
fn a_new_cli_reads_an_old_hub_but_refuses_to_skip_when_filters_would_be_ignored() {
    use axum::{
        Json, Router,
        routing::{get, post},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    let reads = Arc::new(AtomicUsize::new(0));
    let seen_reads = reads.clone();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route(protocol::HUB, get(|| async { Json(serde_json::json!({"version":"old", "build":"old", "started":1})) }))
        .route(protocol::AGENTS, post(|| async { Json(serde_json::json!({"name":"a", "machine":"old", "lastSeen":1, "watching":false, "unread":1, "person":false})) }))
        .route(protocol::INBOX, post(move || {
            let reads = seen_reads.clone();
            async move {
                reads.fetch_add(1, Ordering::SeqCst);
                Json(serde_json::json!({"messages":[{"id":1,"from":"b","to":"a","body":"legacy body must be shown","sentAt":1,"readAt":2,"person":false,"channel":null}],"remaining":0}))
            }
        }));
    thread::spawn(move || runtime.block_on(async { axum::serve(listener, app).await.unwrap() }));
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_cross-agent-message"))
            .args(["--hub", &format!("http://{address}"), "inbox", "--as", "a"])
            .args(args)
            .output()
            .unwrap()
    };
    let normal = run(&[]);
    assert!(normal.status.success());
    assert!(String::from_utf8_lossy(&normal.stdout).contains("legacy body must be shown"));
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    for args in [&["--skip", "#team"][..], &["#team"][..]] {
        let refused = run(args);
        assert!(!refused.status.success());
        assert!(
            String::from_utf8_lossy(&refused.stderr)
                .contains("does not support selective inbox reads")
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "unsupported filters must never acknowledge unseen mail"
        );
    }
}

/// A hub serving the dashboard kept in its binary answers its own address
/// with the page, where it was built with one, and what isn't a file of it
/// with none: a release needs no checkout beside it.
#[test]
fn a_hub_serves_the_dashboard_kept_in_its_binary() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let address = listener.local_addr().unwrap();
    let hub = Hub::new(Store::in_memory().unwrap()).unwrap();
    let app = hub::router(
        Arc::new(hub),
        cross_agent_message::dashboard::Dashboard::Embedded,
    );
    thread::spawn(move || runtime.block_on(async { axum::serve(listener, app).await.unwrap() }));
    let page = ureq::get(&format!("http://{address}/")).call();
    match cross_agent_message::dashboard::embedded() {
        true => {
            let page = page.unwrap();
            assert_eq!(page.status(), 200);
            let kind = page
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap();
            assert_eq!(kind, "text/html; charset=utf-8");
        }
        false => assert!(page.is_err()),
    }
    assert!(
        ureq::get(&format!("http://{address}/no-such-file"))
            .call()
            .is_err()
    );
    // The API is the hub's whatever the dashboard is.
    assert!(
        ureq::get(&format!("http://{address}/api/hub"))
            .call()
            .is_ok()
    );
}

#[test]
fn incompatible_protocols_are_refused_before_any_command_and_legacy_headers_work() {
    let client = hub();
    for version in ["0", "99", "not-a-number"] {
        let http: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut response = http
            .get(format!("{}/api/hub", client.hub()))
            .header(protocol::PROTOCOL_HEADER, version)
            .call()
            .unwrap();
        assert_eq!(response.status().as_u16(), 426);
        let refusal: protocol::Refusal = response.body_mut().read_json().unwrap();
        assert_eq!(refusal.code.as_deref(), Some("upgrade-required"));
    }
    let response = ureq::get(format!("{}/api/hub", client.hub()))
        .call()
        .unwrap();
    assert_eq!(
        response.headers().get(protocol::PROTOCOL_HEADER).unwrap(),
        "1"
    );
}

#[test]
fn legacy_event_subscriptions_never_receive_new_event_types() {
    use std::io::BufRead;
    let client = hub();
    client.register("a").unwrap();
    client.register("b").unwrap();
    let response = ureq::get(format!("{}/api/events?agent=b", client.hub()))
        .call()
        .unwrap();
    client.send("a", "b", "known event").unwrap();
    client.retire("b").unwrap();
    let rows: Vec<String> = std::io::BufReader::new(response.into_body().into_reader())
        .lines()
        .map(Result::unwrap)
        .filter(|line| line.starts_with("data:"))
        .collect();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("known event"));
    assert!(!rows[0].contains("retired"));
}

#[test]
fn an_unknown_sse_event_is_skipped_without_reopening_the_connection() {
    use axum::{Router, routing::get};
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().route(protocol::EVENTS, get(|| async {
        ([("content-type", "text/event-stream")], "data: {\"type\":\"future-event\",\"payload\":123}\n\ndata: {\"type\":\"retired\",\"name\":\"a\"}\n\n")
    }));
    thread::spawn(move || runtime.block_on(async { axum::serve(listener, app).await.unwrap() }));
    let client = Client::new(&format!("http://{address}"));
    let mut events = client.watch("a").unwrap();
    assert_eq!(
        events.next().unwrap().unwrap(),
        protocol::Event::Retired {
            name: "a".to_string()
        }
    );
    assert!(events.next().is_none());
}

#[test]
fn self_update_verifies_and_stages_before_changing_the_executable() {
    let directory = std::env::temp_dir().join(format!("cam-update-test-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let destination = directory.join("cross-agent-message");
    std::fs::write(&destination, b"old executable").unwrap();
    use cross_agent_message::updates::{install, sha256};
    assert!(install(b"bad download", "wrong checksum", &destination).is_err());
    assert_eq!(std::fs::read(&destination).unwrap(), b"old executable");
    assert!(
        install(
            b"not an executable",
            &sha256(b"not an executable"),
            &destination
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&destination).unwrap(), b"old executable");
    let binary = std::fs::read(env!("CARGO_BIN_EXE_cross-agent-message")).unwrap();
    let info = install(&binary, &sha256(&binary), &destination).unwrap();
    assert_eq!(info.protocol, protocol::PROTOCOL);
    assert_eq!(
        std::fs::read(destination.with_extension("prev")).unwrap(),
        b"old executable"
    );
    assert_eq!(std::fs::read(&destination).unwrap(), binary);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn a_cli_that_cannot_print_its_inbox_never_acknowledges_the_mail() {
    let client = hub();
    client.register("a").unwrap();
    client.register("b").unwrap();
    client
        .send("a", "b", "must be printable before acknowledgement")
        .unwrap();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_cross-agent-message"));
    let mut child = command
        .args(["--hub", client.hub(), "inbox", "--as", "b"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdout.take());
    let result = child.wait_with_output().unwrap();
    assert!(!result.status.success());
    assert_eq!(
        client
            .agents()
            .unwrap()
            .iter()
            .find(|a| a.name == "b")
            .unwrap()
            .unread,
        1
    );
    assert_eq!(
        client.read_inbox("b", 20).unwrap().messages[0].body,
        "must be printable before acknowledgement"
    );
}
