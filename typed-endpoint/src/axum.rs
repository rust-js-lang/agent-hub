//! Typed server adapters. Paths, methods and wire types come from the shared contracts;
//! handlers receive decoded queries and bodies, with transport errors refused
//! in the same JSON shape as application errors.
//! A service supplies its state and error type. Handler signatures must match
//! the endpoint body, response and method.
//!
//! ```
//! # use axum::{Json, extract::State, http::{HeaderMap, StatusCode}, response::{IntoResponse, Response}};
//! # use typed_endpoint::{Post, axum::{Routes, TransportError}};
//! # struct Problem;
//! # impl IntoResponse for Problem { fn into_response(self) -> Response { StatusCode::INTERNAL_SERVER_ERROR.into_response() } }
//! # impl TransportError for Problem { fn transport(_: StatusCode, _: String) -> Self { Self } }
//! # async fn echo(_: State<()>, _: HeaderMap, Json(body): Json<String>) -> Result<Json<String>, Problem> { Ok(Json(body)) }
//! const ENDPOINT: Post<String, String> = Post::new("ECHO", "/echo", None);
//! let _: Routes<(), Problem> = Routes::new().post(&ENDPOINT, echo);
//! ```
//!
//! ```compile_fail
//! # use axum::{Json, extract::State, http::{HeaderMap, StatusCode}, response::{IntoResponse, Response}};
//! # use typed_endpoint::{Post, axum::{Routes, TransportError}};
//! # struct Problem;
//! # impl IntoResponse for Problem { fn into_response(self) -> Response { StatusCode::INTERNAL_SERVER_ERROR.into_response() } }
//! # impl TransportError for Problem { fn transport(_: StatusCode, _: String) -> Self { Self } }
//! # async fn echo(_: State<()>, _: HeaderMap, Json(body): Json<String>) -> Result<Json<String>, Problem> { Ok(Json(body)) }
//! const ENDPOINT: Post<u32, String> = Post::new("ECHO", "/echo", None);
//! let _: Routes<(), Problem> = Routes::new().post(&ENDPOINT, echo);
//! ```
//!
//! ```compile_fail
//! # use axum::{Json, extract::State, http::{HeaderMap, StatusCode}, response::{IntoResponse, Response}};
//! # use typed_endpoint::{Post, axum::{Routes, TransportError}};
//! # struct Problem;
//! # impl IntoResponse for Problem { fn into_response(self) -> Response { StatusCode::INTERNAL_SERVER_ERROR.into_response() } }
//! # impl TransportError for Problem { fn transport(_: StatusCode, _: String) -> Self { Self } }
//! # async fn echo(_: State<()>, _: HeaderMap, Json(body): Json<String>) -> Result<Json<String>, Problem> { Ok(Json(body)) }
//! const ENDPOINT: Post<String, u32> = Post::new("ECHO", "/echo", None);
//! let _: Routes<(), Problem> = Routes::new().post(&ENDPOINT, echo);
//! ```
//!
//! ```compile_fail
//! # use axum::{Json, extract::State, http::{HeaderMap, StatusCode}, response::{IntoResponse, Response}};
//! # use typed_endpoint::{Post, axum::{Routes, TransportError}};
//! # struct Problem;
//! # impl IntoResponse for Problem { fn into_response(self) -> Response { StatusCode::INTERNAL_SERVER_ERROR.into_response() } }
//! # impl TransportError for Problem { fn transport(_: StatusCode, _: String) -> Self { Self } }
//! # async fn echo(_: State<()>, _: HeaderMap, Json(body): Json<String>) -> Result<Json<String>, Problem> { Ok(Json(body)) }
//! const ENDPOINT: Post<String, String> = Post::new("ECHO", "/echo", None);
//! let _: Routes<(), Problem> = Routes::new().get(&ENDPOINT, echo);
//! ```
use std::{convert::Infallible, future::Future};

use crate::{
    Download, Get, Kind, Method, Post, Query as WireQuery, Route, Stream as EventEndpoint,
};
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
use serde::{Serialize, de::DeserializeOwned};
use tokio_stream::{Stream, StreamExt};

/// Application-owned JSON/HTTP error mapping for extractor failures.
pub trait TransportError: IntoResponse + Send {
    fn transport(status: StatusCode, reason: String) -> Self;
}

type Pairs = Result<Query<Vec<(String, String)>>, QueryRejection>;

fn query<Q: WireQuery, E: TransportError>(pairs: Pairs) -> Result<Q, E> {
    let Query(pairs) = pairs.map_err(|error| E::transport(error.status(), error.body_text()))?;
    Q::parse(&pairs).map_err(|reason| E::transport(StatusCode::BAD_REQUEST, reason))
}

pub struct Routes<S, E, C = ()> {
    router: Router<S>,
    registered: Vec<Route<C>>,
    validate_stream: fn(&HeaderMap, Option<u32>) -> Result<(), E>,
}

impl<S, E, C> Default for Routes<S, E, C>
where
    S: Clone + Send + Sync + 'static,
    E: TransportError + 'static,
    C: Copy,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S, E, C> Routes<S, E, C>
where
    S: Clone + Send + Sync + 'static,
    E: TransportError + 'static,
    C: Copy,
{
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            registered: Vec::new(),
            validate_stream: |_, _| Ok(()),
        }
    }
    /// Validate application protocol claims before opening a stream.
    pub fn with_stream_validation(
        mut self,
        validate: fn(&HeaderMap, Option<u32>) -> Result<(), E>,
    ) -> Self {
        self.validate_stream = validate;
        self
    }

    pub fn registered(&self) -> &[Route<C>] {
        &self.registered
    }
    pub fn finish(self) -> Router<S> {
        self.router
    }

    fn claim(&mut self, route: Route<C>, method: Method, kind: Kind) {
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

    pub fn get<Q, Res, F, Fut>(mut self, endpoint: &Get<Q, Res, C>, handler: F) -> Self
    where
        Q: WireQuery + Send + 'static,
        Res: Serialize + 'static,
        F: Fn(State<S>, Query<Q>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Json<Res>, E>> + Send + 'static,
    {
        self.claim(endpoint.route, Method::Get, Kind::Json);
        self.router = self.router.route(
            endpoint.route.path,
            get(move |state: State<S>, pairs: Pairs| {
                let handler = handler.clone();
                async move { handler(state, Query(query::<Q, E>(pairs)?)).await }
            }),
        );
        self
    }

    pub fn post<B, Res, F, Fut>(mut self, endpoint: &Post<B, Res, C>, handler: F) -> Self
    where
        B: DeserializeOwned + Send + 'static,
        Res: Serialize + 'static,
        F: Fn(State<S>, HeaderMap, Json<B>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Json<Res>, E>> + Send + 'static,
    {
        self.claim(endpoint.route, Method::Post, Kind::Json);
        self.router = self.router.route(
            endpoint.route.path,
            post(
                move |state: State<S>, headers: HeaderMap, body: Result<Json<B>, JsonRejection>| {
                    let handler = handler.clone();
                    async move {
                        let body =
                            body.map_err(|error| E::transport(error.status(), error.body_text()))?;
                        handler(state, headers, body).await
                    }
                },
            ),
        );
        self
    }

    /// A unit request has no data. Accept the historical empty body as well
    /// as the JSON null emitted by the generic client.
    pub fn post_empty<Res, F, Fut>(mut self, endpoint: &Post<(), Res, C>, handler: F) -> Self
    where
        Res: Serialize + 'static,
        F: Fn(State<S>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Json<Res>, E>> + Send + 'static,
    {
        self.claim(endpoint.route, Method::Post, Kind::Json);
        self.router = self.router.route(endpoint.route.path, post(handler));
        self
    }

    /// Two exact query/response contracts share one HTTP method/path. Their
    /// shared parsers must select exactly one handler, never guess a variant.
    pub fn get_variants<Q1, R1, Q2, R2, F1, F2, A, B>(
        mut self,
        first: &Get<Q1, R1, C>,
        second: &Get<Q2, R2, C>,
        one: F1,
        two: F2,
    ) -> Self
    where
        Q1: WireQuery + Send + 'static,
        Q2: WireQuery + Send + 'static,
        R1: Serialize + 'static,
        R2: Serialize + 'static,
        F1: Fn(State<S>, Query<Q1>) -> A + Clone + Send + Sync + 'static,
        F2: Fn(State<S>, Query<Q2>) -> B + Clone + Send + Sync + 'static,
        A: Future<Output = Result<Json<R1>, E>> + Send + 'static,
        B: Future<Output = Result<Json<R2>, E>> + Send + 'static,
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
            get(move |state: State<S>, pairs: Pairs| {
                let (one, two) = (one.clone(), two.clone());
                async move {
                    let Query(pairs) =
                        pairs.map_err(|error| E::transport(error.status(), error.body_text()))?;
                    match (Q1::parse(&pairs), Q2::parse(&pairs)) {
                        (Ok(value), Err(_)) => {
                            Ok::<Response, E>(one(state, Query(value)).await?.into_response())
                        }
                        (Err(_), Ok(value)) => Ok(two(state, Query(value)).await?.into_response()),
                        (Ok(_), Ok(_)) => Err(E::transport(
                            StatusCode::BAD_REQUEST,
                            "query matches multiple endpoint variants".to_string(),
                        )),
                        (Err(first), Err(second)) => Err(E::transport(
                            StatusCode::BAD_REQUEST,
                            format!("invalid query: {first}; {second}"),
                        )),
                    }
                }
            }),
        );
        self
    }

    pub fn download<Q, F, Fut>(mut self, endpoint: &Download<Q, Vec<u8>, C>, handler: F) -> Self
    where
        Q: WireQuery + Send + 'static,
        F: Fn(State<S>, Query<Q>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<(HeaderMap, Vec<u8>), E>> + Send + 'static,
    {
        self.claim(endpoint.route, Method::Get, Kind::Download);
        self.router = self.router.route(
            endpoint.route.path,
            get(move |state: State<S>, pairs: Pairs| {
                let handler = handler.clone();
                async move { handler(state, Query(query::<Q, E>(pairs)?)).await }
            }),
        );
        self
    }

    pub fn stream<Q, Item, Events, F, Fut>(
        mut self,
        endpoint: &EventEndpoint<Q, Item, C>,
        handler: F,
    ) -> Self
    where
        Q: WireQuery + Send + 'static,
        Item: Serialize + Send + 'static,
        Events: Stream<Item = Result<Item, Infallible>> + Send + 'static,
        F: Fn(State<S>, Query<Q>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Events, E>> + Send + 'static,
    {
        self.claim(endpoint.route, Method::Get, Kind::Stream);
        let validate = self.validate_stream;
        self.router = self.router.route(
            endpoint.route.path,
            get(move |state: State<S>, headers: HeaderMap, pairs: Pairs| {
                let handler = handler.clone();
                async move {
                    let query = query::<Q, E>(pairs)?;
                    validate(&headers, query.protocol())?;
                    let stream = handler(state, Query(query)).await?;
                    let encoded = stream.map(|value| match value {
                        Ok(event) => Event::default().json_data(event),
                        Err(never) => match never {},
                    });
                    Ok::<_, E>(Sse::new(encoded).keep_alive(KeepAlive::default()))
                }
            }),
        );
        self
    }
}
