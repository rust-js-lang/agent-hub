#![cfg(feature = "axum")]

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Query, State},
    http::{HeaderMap, Request, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio_stream::{Stream as EventStream, iter};
use tower::ServiceExt;
use typed_endpoint::{
    Download, Get, Post, Query as WireQuery, Stream,
    axum::{Routes, TransportError},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Capability {
    Write,
}
const READ: Get<Search, Value, Capability> = Get::new("READ", "/value", None);
const WRITE: Post<Value, Value, Capability> = Post::new("WRITE", "/value", Some(Capability::Write));
const CLEAR: Post<(), Value, Capability> = Post::new("CLEAR", "/clear", None);
const FILE: Download<(), Vec<u8>, Capability> = Download::new("FILE", "/file", None);
const EVENTS: Stream<(), Value, Capability> = Stream::new("EVENTS", "/events", None);

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Value {
    text: String,
}

struct Search {
    text: String,
}
impl WireQuery for Search {
    fn pairs(&self) -> Vec<(&'static str, String)> {
        vec![("text", self.text.clone())]
    }
    fn parse(pairs: &[(String, String)]) -> Result<Self, String> {
        let values: Vec<_> = pairs.iter().filter(|(key, _)| key == "text").collect();
        match values.as_slice() {
            [(_, text)] => Ok(Self { text: text.clone() }),
            _ => Err("exactly one text required".into()),
        }
    }
}

#[derive(Clone, Default)]
struct Service {
    calls: Arc<AtomicUsize>,
}

struct Problem(StatusCode, String);
impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({"problem": self.1}))).into_response()
    }
}
impl TransportError for Problem {
    fn transport(status: StatusCode, reason: String) -> Self {
        Self(status, reason)
    }
}

async fn read(_: State<Service>, Query(query): Query<Search>) -> Result<Json<Value>, Problem> {
    Ok(Json(Value { text: query.text }))
}
async fn write(
    State(state): State<Service>,
    _: HeaderMap,
    Json(value): Json<Value>,
) -> Result<Json<Value>, Problem> {
    state.calls.fetch_add(1, Ordering::SeqCst);
    if value.text == "refuse" {
        Err(Problem(StatusCode::CONFLICT, "business refusal".into()))
    } else {
        Ok(Json(value))
    }
}
async fn clear(_: State<Service>) -> Result<Json<Value>, Problem> {
    Ok(Json(Value {
        text: "cleared".into(),
    }))
}
async fn file(_: State<Service>, _: Query<()>) -> Result<(HeaderMap, Vec<u8>), Problem> {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/octet-stream".parse().unwrap());
    Ok((headers, vec![0, 255, 1]))
}
async fn events(
    State(state): State<Service>,
    _: Query<()>,
) -> Result<impl EventStream<Item = Result<Value, Infallible>>, Problem> {
    state.calls.fetch_add(1, Ordering::SeqCst);
    Ok(iter([Ok(Value {
        text: "event".into(),
    })]))
}
fn validate(headers: &HeaderMap, _: Option<u32>) -> Result<(), Problem> {
    if headers.get("x-version").is_some_and(|value| value != "1") {
        Err(Problem(
            StatusCode::UPGRADE_REQUIRED,
            "wrong version".into(),
        ))
    } else {
        Ok(())
    }
}
fn routes() -> Routes<Service, Problem, Capability> {
    Routes::new()
        .with_stream_validation(validate)
        .get(&READ, read)
        .post(&WRITE, write)
        .post_empty(&CLEAR, clear)
        .download(&FILE, file)
        .stream(&EVENTS, events)
}
fn app(state: Service) -> Router {
    routes().finish().with_state(state)
}
async fn bytes(response: Response) -> Vec<u8> {
    to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap()
        .to_vec()
}

#[tokio::test]
async fn independent_service_uses_its_own_state_queries_errors_and_payloads() {
    let state = Service::default();
    let app = app(state.clone());
    assert_eq!(routes().registered()[1].needs, Some(Capability::Write));
    let uri = READ.url(&Search {
        text: "a + &= / Việt".into(),
    });
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes(response).await)
            .unwrap()
            .text,
        "a + &= / Việt"
    );
    for uri in ["/value", "/value?text=a&text=b"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes(response).await).unwrap()["problem"],
            "exactly one text required"
        );
    }
    for (body, status) in [
        ("not JSON", StatusCode::BAD_REQUEST),
        (r#"{"text":"ok"}"#, StatusCode::OK),
        (r#"{"text":"refuse"}"#, StatusCode::CONFLICT),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(WRITE.url())
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        let value: serde_json::Value = serde_json::from_slice(&bytes(response).await).unwrap();
        assert!(
            value
                .get(if status.is_success() {
                    "text"
                } else {
                    "problem"
                })
                .is_some()
        );
    }
    assert_eq!(
        state.calls.load(Ordering::SeqCst),
        2,
        "bad JSON never invokes the handler"
    );
    for body in ["", "null"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(CLEAR.url())
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri(FILE.url(&()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.headers()["content-type"],
        "application/octet-stream"
    );
    assert_eq!(bytes(response).await, vec![0, 255, 1]);
}

#[tokio::test]
async fn stream_validation_precedes_handler_and_events_are_json_sse() {
    let state = Service::default();
    let app = app(state.clone());
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(EVENTS.url(&()))
                .header("x-version", "2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
    assert_eq!(state.calls.load(Ordering::SeqCst), 0);
    let response = app
        .oneshot(
            Request::builder()
                .uri(EVENTS.url(&()))
                .header("x-version", "1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert_eq!(
        String::from_utf8(bytes(response).await).unwrap(),
        "data: {\"text\":\"event\"}\n\n"
    );
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn query_variants_require_exactly_one_parser_to_match() {
    struct Alternative;
    impl WireQuery for Alternative {
        fn pairs(&self) -> Vec<(&'static str, String)> {
            vec![("other", "yes".into())]
        }
        fn parse(pairs: &[(String, String)]) -> Result<Self, String> {
            if pairs.iter().any(|(key, _)| key == "other") {
                Ok(Self)
            } else {
                Err("missing other".into())
            }
        }
    }
    async fn alternative(_: State<Service>, _: Query<Alternative>) -> Result<Json<u32>, Problem> {
        Ok(Json(42))
    }
    const OTHER: Get<Alternative, u32, Capability> = Get::new("OTHER", "/value", None);
    let app = Routes::new()
        .get_variants(&READ, &OTHER, read, alternative)
        .finish()
        .with_state(Service::default());
    for (query, status, expected) in [
        (
            "?text=hello",
            StatusCode::OK,
            serde_json::json!({"text":"hello"}),
        ),
        ("?other=yes", StatusCode::OK, serde_json::json!(42)),
        (
            "?text=hello&other=yes",
            StatusCode::BAD_REQUEST,
            serde_json::json!({"problem":"query matches multiple endpoint variants"}),
        ),
        (
            "",
            StatusCode::BAD_REQUEST,
            serde_json::json!({"problem":"invalid query: exactly one text required; missing other"}),
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/value{query}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes(response).await).unwrap(),
            expected
        );
    }
}

#[test]
#[should_panic(expected = "duplicate endpoint registration")]
fn duplicate_method_and_path_are_rejected() {
    let _ = routes().get(&READ, read);
}

#[test]
#[should_panic(expected = "duplicate query variant")]
fn duplicate_query_variant_is_rejected() {
    let _ = Routes::new().get_variants(&READ, &READ, read, read);
}

#[test]
#[should_panic(expected = "endpoint method disagrees with its transport")]
fn changing_descriptor_metadata_cannot_change_transport_kind() {
    let mut read_endpoint = Get::<Search, Value, Capability>::new("READ", "/value", None);
    read_endpoint.route.method = typed_endpoint::Method::Post;
    let _: Routes<Service, Problem, Capability> = Routes::new().get(&read_endpoint, read);
}
