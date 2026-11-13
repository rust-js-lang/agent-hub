//! Hub policy for the reusable typed HTTP server adapters.
use std::sync::Arc;

use crate::hub::{Hub, Refused};
use axum::http::{HeaderMap, StatusCode};
use protocol::endpoints::Feature;

/// ```compile_fail
/// use std::sync::Arc;
/// use axum::{Json, extract::State, http::HeaderMap};
/// use agent_hub::{hub::{Hub, Refused}, routes::Routes};
/// async fn wrong(_: State<Arc<Hub>>, _: HeaderMap, _: Json<protocol::Register>) -> Result<Json<protocol::Message>, Refused> { unreachable!() }
/// Routes::new().post(&protocol::endpoints::SEND, wrong);
/// ```
/// ```compile_fail
/// use std::sync::Arc;
/// use axum::{Json, extract::State, http::HeaderMap};
/// use agent_hub::{hub::{Hub, Refused}, routes::Routes};
/// async fn wrong(_: State<Arc<Hub>>, _: HeaderMap, _: Json<protocol::Send>) -> Result<Json<protocol::Agent>, Refused> { unreachable!() }
/// Routes::new().post(&protocol::endpoints::SEND, wrong);
/// ```
pub type Routes = typed_endpoint::axum::Routes<Arc<Hub>, Refused, Feature>;

impl typed_endpoint::axum::TransportError for Refused {
    fn transport(status: StatusCode, reason: String) -> Self {
        Self(status, reason)
    }
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
                "This hub speaks protocol {}; this client claims {}. Upgrade the client: agent-hub upgrade-self",
                protocol::PROTOCOL,
                version.map_or("invalid".to_string(), |value| value.to_string())
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        body::{Body, to_bytes},
        extract::{Query, State},
        http::Request,
        response::Response,
    };
    use protocol::endpoints as api;
    use protocol::endpoints::Method;
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
