//! The typed native transport against deliberately old and refusing hubs.

use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use cross_agent_message::client::Client;
use protocol::endpoints::{self as api, Code, Refused};

#[derive(Debug)]
struct Received {
    method: String,
    path: String,
    query: String,
    protocol_header: Option<String>,
}

struct Fixture {
    client: Client,
    received: Arc<Mutex<Vec<Received>>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Fixture {
    fn new(features: &[&str], status: StatusCode, answer: &str) -> Self {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let address = listener.local_addr().unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let requests = received.clone();
        let metadata =
            serde_json::json!({"version":"old", "build":"old", "started":1, "features": features})
                .to_string();
        let answer = answer.to_string();
        let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
            let (requests, metadata, answer) = (requests.clone(), metadata.clone(), answer.clone());
            async move {
                let path = request.uri().path().to_string();
                requests.lock().unwrap().push(Received {
                    method: request.method().to_string(),
                    path: path.clone(),
                    query: request.uri().query().unwrap_or_default().to_string(),
                    protocol_header: request
                        .headers()
                        .get(protocol::PROTOCOL_HEADER)
                        .map(|v| v.to_str().unwrap().to_string()),
                });
                // Consume the body just as a real JSON extractor would.
                axum::body::to_bytes(request.into_body(), 1024 * 1024)
                    .await
                    .unwrap();
                let (status, body) = if path == "/api/hub" {
                    (StatusCode::OK, metadata)
                } else {
                    (status, answer)
                };
                (status, [(protocol::PROTOCOL_HEADER, "1")], body)
            }
        });
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            runtime.block_on(async {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            })
        });
        Self {
            client: Client::new(&format!("http://{address}")),
            received,
            stop: Some(stop),
            thread: Some(thread),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.thread.take().unwrap().join().unwrap();
    }
}

#[test]
fn unsupported_inbox_behavior_is_never_sent_even_when_only_sender_is_set() {
    let fixture = Fixture::new(&[], StatusCode::OK, r#"{"messages":[],"remaining":0}"#);
    for json in [
        r#"{"name":"a","max":10,"sender":"b"}"#,
        r#"{"name":"a","max":10,"channel":"team"}"#,
        r#"{"name":"a","max":10,"digest":true}"#,
        r#"{"name":"a","max":10,"skip":true}"#,
    ] {
        let read: protocol::ReadInbox = serde_json::from_str(json).unwrap();
        let error = fixture.client.read_mail(&read).unwrap_err().to_string();
        assert!(error.contains("inbox-digests"), "{error}");
    }
    let read: protocol::ReadInbox =
        serde_json::from_str(r#"{"name":"a","max":10,"sender":"b","defer_ack":true,"up_to":8}"#)
            .unwrap();
    let error = fixture.client.read_mail(&read).unwrap_err().to_string();
    for feature in ["inbox-digests", "read-ack", "bounded-skip"] {
        assert!(error.contains(feature), "{error}");
    }
    let requests = fixture.received.lock().unwrap();
    assert_eq!(requests.len(), 5);
    assert!(
        requests
            .iter()
            .all(|request| request.method == "GET" && request.path == "/api/hub")
    );
}

#[test]
fn original_inbox_request_still_works_without_new_features() {
    let fixture = Fixture::new(&[], StatusCode::OK, r#"{"messages":[],"remaining":0}"#);
    assert!(
        fixture
            .client
            .read_inbox("a", 10)
            .unwrap()
            .messages
            .is_empty()
    );
    let requests = fixture.received.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/api/inbox");
    assert_eq!(requests[0].protocol_header.as_deref(), Some("1"));
}

#[test]
fn refusal_status_and_code_reach_the_native_caller() {
    let fixture = Fixture::new(
        &[],
        StatusCode::GONE,
        r#"{"reason":"retired before codes existed"}"#,
    );
    let error = fixture.client.agents().unwrap_err();
    let refusal = error.downcast_ref::<Refused>().unwrap();
    assert_eq!(refusal.status, 410);
    assert_eq!(refusal.code(), Code::Retired);
    assert_eq!(error.to_string(), "retired before codes existed");
}

#[test]
fn streams_put_protocol_in_the_query_and_decode_complete_frames() {
    let fixture = Fixture::new(
        &[],
        StatusCode::OK,
        ": comment\ndata: {\ndata: \"type\":\"restarting\"}\n\n",
    );
    let query = api::Events {
        agent: Some("b".to_string()),
        protocol: Some(1),
        types: Some(vec!["restarting".to_string()]),
    };
    let events = fixture
        .client
        .stream(&api::EVENTS, &query)
        .unwrap()
        .collect::<anyhow::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(events, vec![protocol::Event::Restarting]);
    let requests = fixture.received.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/api/events");
    assert_eq!(requests[0].query, "agent=b&protocol=1&types=restarting");
    assert_eq!(requests[0].protocol_header, None);
}

#[test]
fn refused_streams_retain_the_typed_upgrade_required_reason() {
    let fixture = Fixture::new(
        &[],
        StatusCode::UPGRADE_REQUIRED,
        r#"{"reason":"upgrade first","code":"upgrade-required"}"#,
    );
    let error = match fixture.client.watch("b") {
        Ok(_) => panic!("stream was refused"),
        Err(error) => error,
    };
    let refusal = error.downcast_ref::<Refused>().unwrap();
    assert_eq!(refusal.status, 426);
    assert_eq!(refusal.code(), Code::UpgradeRequired);
}
