//! Typed server adapters. Paths, methods and wire types come from protocol;
//! handlers receive decoded queries and bodies, with transport errors refused
//! in the same JSON shape as application errors.
use std::{convert::Infallible, future::Future, sync::Arc};

use axum::{
    Json, Router,
    extract::{
        Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use protocol::endpoints::{
    Download, Get, Kind, Method, Post, Query as WireQuery, Route, Stream as EventEndpoint,
};
use serde::{Serialize, de::DeserializeOwned};
use tokio_stream::{Stream, StreamExt};

use crate::hub::{Hub, Refused};

type Pairs = Result<Query<Vec<(String, String)>>, QueryRejection>;

fn query<Q: WireQuery>(pairs: Pairs) -> Result<Q, Refused> {
    let Query(pairs) = pairs.map_err(|error| Refused(error.status(), error.body_text()))?;
    Q::parse(&pairs).map_err(|reason| Refused(StatusCode::BAD_REQUEST, reason))
}

/// Resolve protocol claims before any handler can change the hub. A stream
/// may use either source, but conflicting header/query claims are a bug.
pub(crate) fn protocol(headers: &HeaderMap, query: Option<u32>) -> Result<(), Refused> {
    let header = headers.get(protocol::PROTOCOL_HEADER);
    let parsed = header
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u32>().ok());
    if let (Some(header), Some(query)) = (parsed, query)
        && header != query
    {
        return Err(Refused(
            StatusCode::BAD_REQUEST,
            "protocol header and stream query disagree".to_string(),
        ));
    }
    let version = if header.is_some() {
        parsed
    } else {
        // Requests without any claim belong to the original wire protocol.
        Some(query.unwrap_or(1))
    };
    if version != Some(protocol::PROTOCOL) {
        return Err(Refused(
            StatusCode::UPGRADE_REQUIRED,
            format!(
                "This hub speaks protocol {}; this client claims {}. Upgrade the client: cross-agent-message upgrade-self",
                protocol::PROTOCOL,
                version.map_or("invalid".to_string(), |value| value.to_string())
            ),
        ));
    }
    Ok(())
}

pub struct Routes {
    router: Router<Arc<Hub>>,
    registered: Vec<Route>,
}

impl Default for Routes {
    fn default() -> Self {
        Self::new()
    }
}

impl Routes {
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            registered: Vec::new(),
        }
    }
    pub fn registered(&self) -> &[Route] {
        &self.registered
    }
    pub fn finish(self) -> Router<Arc<Hub>> {
        self.router
    }

    fn claim(&mut self, route: Route, method: Method, kind: Kind) {
        assert_eq!(
            route.method, method,
            "endpoint method disagrees with its transport"
        );
        assert_eq!(
            route.kind, kind,
            "endpoint kind disagrees with its transport"
        );
        // Keep this exhaustive when another transport kind is introduced.
        match route.kind {
            Kind::Json => {}
            Kind::Stream | Kind::Download => assert_eq!(route.method, Method::Get),
        }
        assert!(
            !self
                .registered
                .iter()
                .any(|old| old.method == route.method && old.path == route.path),
            "duplicate endpoint registration: {}",
            route.path
        );
        self.registered.push(route);
    }

    pub fn get<Q, Res, F, Fut>(mut self, endpoint: &Get<Q, Res>, handler: F) -> Self
    where
        Q: WireQuery + Send + 'static,
        Res: Serialize + 'static,
        F: Fn(State<Arc<Hub>>, Query<Q>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Json<Res>, Refused>> + Send + 'static,
    {
        self.claim(endpoint.route, Method::Get, Kind::Json);
        self.router = self.router.route(
            endpoint.route.path,
            get(move |hub: State<Arc<Hub>>, pairs: Pairs| {
                let handler = handler.clone();
                async move { handler(hub, Query(query::<Q>(pairs)?)).await }
            }),
        );
        self
    }

    /// ```compile_fail
    /// use std::sync::Arc;
    /// use axum::{Json, extract::State, http::HeaderMap};
    /// use cross_agent_message::{hub::{Hub, Refused}, routes::Routes};
    /// async fn wrong(_: State<Arc<Hub>>, _: HeaderMap, _: Json<protocol::Register>) -> Result<Json<protocol::Message>, Refused> { unreachable!() }
    /// Routes::new().post(&protocol::endpoints::SEND, wrong);
    /// ```
    /// ```compile_fail
    /// use std::sync::Arc;
    /// use axum::{Json, extract::State, http::HeaderMap};
    /// use cross_agent_message::{hub::{Hub, Refused}, routes::Routes};
    /// async fn wrong(_: State<Arc<Hub>>, _: HeaderMap, _: Json<protocol::Send>) -> Result<Json<protocol::Agent>, Refused> { unreachable!() }
    /// Routes::new().post(&protocol::endpoints::SEND, wrong);
    /// ```
    pub fn post<B, Res, F, Fut>(mut self, endpoint: &Post<B, Res>, handler: F) -> Self
    where
        B: DeserializeOwned + Send + 'static,
        Res: Serialize + 'static,
        F: Fn(State<Arc<Hub>>, HeaderMap, Json<B>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Json<Res>, Refused>> + Send + 'static,
    {
        self.claim(endpoint.route, Method::Post, Kind::Json);
        self.router = self.router.route(
            endpoint.route.path,
            post(
                move |hub: State<Arc<Hub>>,
                      headers: HeaderMap,
                      body: Result<Json<B>, JsonRejection>| {
                    let handler = handler.clone();
                    async move {
                        let body =
                            body.map_err(|error| Refused(error.status(), error.body_text()))?;
                        handler(hub, headers, body).await
                    }
                },
            ),
        );
        self
    }

    /// A unit request has no data. Accept the historical empty body as well
    /// as the JSON null emitted by the generic client.
    pub fn post_empty<Res, F, Fut>(mut self, endpoint: &Post<(), Res>, handler: F) -> Self
    where
        Res: Serialize + 'static,
        F: Fn(State<Arc<Hub>>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Json<Res>, Refused>> + Send + 'static,
    {
        self.claim(endpoint.route, Method::Post, Kind::Json);
        self.router = self.router.route(endpoint.route.path, post(handler));
        self
    }

    /// Two exact query/response contracts share one HTTP method/path. Their
    /// shared parsers must select exactly one handler, never guess a variant.
    pub fn get_variants<Q1, R1, Q2, R2, F1, F2, A, B>(
        mut self,
        first: &Get<Q1, R1>,
        second: &Get<Q2, R2>,
        one: F1,
        two: F2,
    ) -> Self
    where
        Q1: WireQuery + Send + 'static,
        Q2: WireQuery + Send + 'static,
        R1: Serialize + 'static,
        R2: Serialize + 'static,
        F1: Fn(State<Arc<Hub>>, Query<Q1>) -> A + Clone + Send + Sync + 'static,
        F2: Fn(State<Arc<Hub>>, Query<Q2>) -> B + Clone + Send + Sync + 'static,
        A: Future<Output = Result<Json<R1>, Refused>> + Send + 'static,
        B: Future<Output = Result<Json<R2>, Refused>> + Send + 'static,
    {
        self.claim(first.route, Method::Get, Kind::Json);
        assert_eq!(first.route.path, second.route.path);
        assert_eq!(second.route.method, Method::Get);
        assert_eq!(second.route.kind, Kind::Json);
        assert!(
            !self
                .registered
                .iter()
                .any(|old| old.name == second.route.name),
            "duplicate query variant"
        );
        self.registered.push(second.route);
        self.router = self.router.route(
            first.route.path,
            get(move |hub: State<Arc<Hub>>, pairs: Pairs| {
                let (one, two) = (one.clone(), two.clone());
                async move {
                    let Query(pairs) =
                        pairs.map_err(|error| Refused(error.status(), error.body_text()))?;
                    match (Q1::parse(&pairs), Q2::parse(&pairs)) {
                        (Ok(value), Err(_)) => {
                            Ok::<Response, Refused>(one(hub, Query(value)).await?.into_response())
                        }
                        (Err(_), Ok(value)) => Ok(two(hub, Query(value)).await?.into_response()),
                        (Ok(_), Ok(_)) => Err(Refused(
                            StatusCode::BAD_REQUEST,
                            "query matches multiple endpoint variants".to_string(),
                        )),
                        (Err(first), Err(second)) => Err(Refused(
                            StatusCode::BAD_REQUEST,
                            format!("invalid query: {first}; {second}"),
                        )),
                    }
                }
            }),
        );
        self
    }

    pub fn download<Q, F, Fut>(mut self, endpoint: &Download<Q, Vec<u8>>, handler: F) -> Self
    where
        Q: WireQuery + Send + 'static,
        F: Fn(State<Arc<Hub>>, Query<Q>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<(HeaderMap, Vec<u8>), Refused>> + Send + 'static,
    {
        self.claim(endpoint.route, Method::Get, Kind::Download);
        self.router = self.router.route(
            endpoint.route.path,
            get(move |hub: State<Arc<Hub>>, pairs: Pairs| {
                let handler = handler.clone();
                async move { handler(hub, Query(query::<Q>(pairs)?)).await }
            }),
        );
        self
    }

    pub fn stream<Q, E, S, F, Fut>(mut self, endpoint: &EventEndpoint<Q, E>, handler: F) -> Self
    where
        Q: WireQuery + Send + 'static,
        E: Serialize + Send + 'static,
        S: Stream<Item = Result<E, Infallible>> + Send + 'static,
        F: Fn(State<Arc<Hub>>, Query<Q>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<S, Refused>> + Send + 'static,
    {
        self.claim(endpoint.route, Method::Get, Kind::Stream);
        self.router = self.router.route(
            endpoint.route.path,
            get(
                move |hub: State<Arc<Hub>>, headers: HeaderMap, pairs: Pairs| {
                    let handler = handler.clone();
                    async move {
                        let query = query::<Q>(pairs)?;
                        protocol(&headers, query.protocol())?;
                        let stream = handler(hub, Query(query)).await?;
                        let encoded = stream.map(|value| match value {
                            Ok(event) => Event::default().json_data(event),
                            Err(never) => match never {},
                        });
                        Ok::<_, Refused>(Sse::new(encoded).keep_alive(KeepAlive::default()))
                    }
                },
            ),
        );
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use protocol::endpoints as api;
    use tower::ServiceExt;

    async fn agents(
        _: State<Arc<Hub>>,
        _: Query<()>,
    ) -> Result<Json<Vec<protocol::Agent>>, Refused> {
        Ok(Json(Vec::new()))
    }

    fn app() -> Router {
        let store = crate::store::Store::in_memory().unwrap();
        store
            .see(
                &protocol::Register {
                    name: "a".to_string(),
                    machine: "test".to_string(),
                    reconnect: false,
                },
                crate::hub::now(),
            )
            .unwrap();
        crate::hub::router(
            Arc::new(Hub::new(store).unwrap()),
            crate::dashboard::Dashboard::Embedded,
        )
    }

    async fn refusal(response: Response) -> protocol::Refusal {
        serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
    }

    #[test]
    fn actual_registry_is_exactly_the_shared_definition_table() {
        let routes = crate::hub::api_routes();
        let mut actual = routes.registered().to_vec();
        let mut expected = api::ALL.to_vec();
        actual.sort_by_key(|route| route.name);
        expected.sort_by_key(|route| route.name);
        assert_eq!(actual, expected);
    }

    #[test]
    #[should_panic(expected = "duplicate endpoint registration")]
    fn duplicate_method_path_registration_is_rejected() {
        let _ = Routes::new()
            .get(&api::AGENTS, agents)
            .get(&api::AGENTS, agents);
    }

    #[test]
    #[should_panic(expected = "duplicate query variant")]
    fn a_query_variant_cannot_register_itself_twice() {
        let _ = Routes::new().get_variants(&api::AGENTS, &api::AGENTS, agents, agents);
    }

    #[tokio::test]
    async fn every_defined_method_and_path_reaches_a_handler() {
        let app = app();
        for route in api::ALL {
            let method = match route.method {
                Method::Get => "GET",
                Method::Post => "POST",
            };
            // Intentionally absent request fields produce extractor refusals,
            // which still prove routing independently of business fixtures.
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(route.path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_ne!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{} {method} {}",
                route.name,
                route.path
            );
            assert_ne!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{} {method} {}",
                route.name,
                route.path
            );
        }
    }

    #[tokio::test]
    async fn malformed_queries_and_json_keep_typed_refusal_bodies() {
        let app = app();
        for path in [
            "/api/messages?limit=-1",
            "/api/messages?limit=1&limit=2",
            "/api/inbox/notice?name=a&with_id=wrong",
            "/api/events?protocol=bad",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(!refusal(response).await.reason.is_empty());
        }
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(api::SEND.route.path)
                    .header("content-type", "application/json")
                    .body(Body::from("not JSON"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!refusal(response).await.reason.is_empty());
    }

    #[tokio::test]
    async fn notice_variants_keep_their_exact_legacy_and_snapshot_wire_shapes() {
        let app = app();
        for (path, snapshot) in [
            (
                api::NOTICE.url(&api::NoticeQuery {
                    name: "a".to_string(),
                    channels: false,
                }),
                false,
            ),
            (
                api::NOTICE_SNAPSHOT.url(&api::NoticeSnapshot {
                    name: "a".to_string(),
                    channels: false,
                }),
                true,
            ),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let value: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
                    .unwrap();
            if snapshot {
                assert_eq!(
                    value,
                    serde_json::json!({"senders":[], "newest_unread_id":0})
                );
            } else {
                assert_eq!(value, serde_json::json!([]));
            }
        }
    }

    #[tokio::test]
    async fn conflicting_stream_claims_are_refused_before_opening_a_watch() {
        let app = app();
        for (header, query, status) in [
            (None, 2, StatusCode::UPGRADE_REQUIRED),
            (Some("1"), 2, StatusCode::BAD_REQUEST),
            (Some("2"), 1, StatusCode::BAD_REQUEST),
            (Some("2"), 2, StatusCode::UPGRADE_REQUIRED),
        ] {
            let mut request =
                Request::builder().uri(format!("/api/events?agent=a&protocol={query}"));
            if let Some(header) = header {
                request = request.header(protocol::PROTOCOL_HEADER, header);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), status);
            assert!(!refusal(response).await.reason.is_empty());
        }
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(api::AGENTS.route.path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let agents: Vec<protocol::Agent> =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
                .unwrap();
        assert!(
            !agents
                .iter()
                .find(|agent| agent.name == "a")
                .unwrap()
                .watching
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/events?agent=a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "legacy EventSource queries remain supported"
        );
    }

    #[tokio::test]
    async fn ambiguous_query_variants_are_refused_instead_of_guessed() {
        let mut alias = api::AGENTS;
        alias.route.name = "AGENTS_ALIAS";
        let store = crate::store::Store::in_memory().unwrap();
        let app = Routes::new()
            .get_variants(&api::AGENTS, &alias, agents, agents)
            .finish()
            .with_state(Arc::new(Hub::new(store).unwrap()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri(api::AGENTS.route.path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            refusal(response)
                .await
                .reason
                .contains("multiple endpoint variants")
        );
    }
}
