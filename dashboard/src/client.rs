//! Browser transport for the same endpoint contracts used by the native CLI.
use js::{date, settle};
use protocol::endpoints::{self as api, Feature, Get, Post, Query, Request, Retry, Route};
use serde::{Serialize, de::DeserializeOwned};
use webapi::{
    BodyInit, EventSource, EventTargetExt, MessageEvent, RequestInit, abort_signal, event_source,
    events, headers, window,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Transport,
    Encode(String),
    Refused(api::Refused),
    Missing(Vec<String>),
}

impl Error {
    pub fn message(self) -> String {
        match self {
            Self::Transport => {
                "The hub can't be reached: is `cross-agent-message serve` running?".to_string()
            }
            Self::Encode(reason) => format!("This page can't write what it sends: {reason}"),
            Self::Refused(refused) => refused.refusal.reason,
            Self::Missing(features) => format!(
                "This hub lacks {}: upgrade it on its machine.",
                features.join(", ")
            ),
        }
    }

    pub fn upgrade_required(&self) -> bool {
        matches!(self, Self::Refused(refused) if refused.code() == api::Code::UpgradeRequired)
    }
}

fn incompatible_protocol() -> Error {
    Error::Refused(api::Refused {
        status: 426,
        refusal: protocol::Refusal {
            code: Some("upgrade-required".to_string()),
            reason: "This page and the hub speak different protocols; reload the page.".to_string(),
        },
    })
}

async fn ask<Res: DeserializeOwned>(
    route: &Route<Feature>,
    path: &str,
    json: Option<&str>,
    nonce: Option<&str>,
    timeout_ms: f64,
) -> Result<Res, Error> {
    let headers = headers::new(None);
    headers.append(protocol::PROTOCOL_HEADER, &protocol::PROTOCOL.to_string());
    if json.is_some() {
        headers.append("content-type", "application/json");
    }
    if let Some(nonce) = nonce {
        headers.append(protocol::WRITE_NONCE_HEADER, nonce);
    }
    let init = RequestInit {
        method: Some(match route.method {
            api::Method::Get => "GET",
            api::Method::Post => "POST",
        }),
        headers: Some(headers),
        body: json.map(|body| Some(BodyInit::Str(body))),
        signal: Some(Some(abort_signal::timeout(timeout_ms.max(1.0)))),
        ..Default::default()
    };
    let response = settle(window.fetch(path, Some(init)))
        .await
        .map_err(|_| Error::Transport)?;
    if response
        .headers()
        .get(protocol::PROTOCOL_HEADER)
        .is_some_and(|value| value.parse::<u32>().ok() != Some(protocol::PROTOCOL))
    {
        return Err(incompatible_protocol());
    }
    let status = response.status() as u16;
    let text = settle(response.text())
        .await
        .map_err(|_| Error::Transport)?;
    api::read(status, &text).map_err(Error::Refused)
}

// The metadata request is a transport operation, so feature negotiation does
// not recursively call get() while get() is negotiating its own features.
async fn metadata() -> Result<protocol::HubInfo, Error> {
    let info: protocol::HubInfo =
        ask(&api::HUB.route, &api::HUB.url(&()), None, None, 15_000.0).await?;
    if info.protocol != protocol::PROTOCOL {
        return Err(incompatible_protocol());
    }
    Ok(info)
}

/// A compatible hub description obtained by a successful probe. Keeping its
/// field private makes feature negotiation a prerequisite of stream creation.
/// ```compile_fail
/// let _ = dashboard::client::Advertised { info: todo!() };
/// ```
pub struct Advertised {
    info: protocol::HubInfo,
}

impl Advertised {
    pub fn info(&self) -> &protocol::HubInfo {
        &self.info
    }
}

pub async fn probe() -> Result<Advertised, Error> {
    Ok(Advertised {
        info: metadata().await?,
    })
}

fn require(
    route: &Route<Feature>,
    request: &impl Request,
    info: &protocol::HubInfo,
) -> Result<(), Error> {
    let missing = api::missing(route, request, &info.features);
    if missing.is_empty() {
        Ok(())
    } else {
        Err(Error::Missing(
            missing
                .iter()
                .map(|feature| feature.name().to_string())
                .collect(),
        ))
    }
}

/// ```compile_fail
/// use dashboard::client;
/// client::get(&protocol::endpoints::SEND, &());
/// ```
pub async fn get<Q: Query + Request, Res: DeserializeOwned>(
    endpoint: &Get<Q, Res, Feature>,
    query: &Q,
) -> Result<Res, Error> {
    if endpoint.route.needs.is_some() || !query.needs().is_empty() {
        require(&endpoint.route, query, &metadata().await?)?;
    }
    ask(&endpoint.route, &endpoint.url(query), None, None, 15_000.0).await
}

/// ```compile_fail
/// use dashboard::client;
/// use protocol::{Register, endpoints::SEND};
/// client::post(&SEND, &Register { name: "a".into(), machine: "m".into(), reconnect: false });
/// ```
/// ```compile_fail
/// use dashboard::client::{self, Error};
/// use protocol::{Agent, Send, endpoints::SEND};
/// async fn wrong() -> Result<Agent, Error> {
///     client::post(&SEND, &Send { from: "a".into(), to: "b".into(), body: "hi".into(), key: None }).await
/// }
/// ```
pub async fn post<B: Serialize + Request, Res: DeserializeOwned>(
    endpoint: &Post<B, Res, Feature>,
    body: &B,
) -> Result<Res, Error> {
    let needs = endpoint.route.needs.is_some() || !body.needs().is_empty();
    let info = if needs || matches!(body.retry(), Retry::Feature(_)) {
        match metadata().await {
            Ok(info) => Some(info),
            Err(error) if needs || error.upgrade_required() => return Err(error),
            Err(_) => None, // No feature proof: one attempt, with no ambiguous replay.
        }
    } else {
        None
    };
    if let Some(info) = &info {
        require(&endpoint.route, body, info)?;
    }
    let replay = match body.retry() {
        Retry::Never => false,
        Retry::Always => true,
        Retry::Feature(feature) => info
            .as_ref()
            .is_some_and(|info| info.features.iter().any(|name| name == feature.name())),
    };
    let nonce_supported = info.as_ref().is_some_and(|info| {
        info.features
            .iter()
            .any(|name| name == api::Feature::WriteNonce.name())
    });
    // Serialize once, and retain one nonce through every ambiguous replay.
    let json = serde_json::to_string(body).map_err(|error| Error::Encode(error.to_string()))?;
    let nonce = format!("{}-{}", date::now(), js::math::random());
    let deadline = date::now() + 15_000.0;
    let attempts = if replay { 3 } else { 1 };
    for attempt in 0..attempts {
        let result = ask(
            &endpoint.route,
            endpoint.url(),
            Some(&json),
            if nonce_supported { Some(&nonce) } else { None },
            deadline - date::now(),
        )
        .await;
        match result {
            Err(Error::Transport) if attempt + 1 < attempts && date::now() < deadline => {}
            result => return result,
        }
    }
    Err(Error::Transport)
}

/// EventSource assembles multiline SSE data before this shared decoder runs.
/// Unknown types are additive; malformed known types reach the caller as an
/// error without replacing a working connection.
pub fn stream<Q: Query + Request>(
    endpoint: &api::Stream<Q, protocol::Event, Feature>,
    query: &Q,
    advertised: &Advertised,
    receive: impl Fn(&'static EventSource, Result<protocol::Event, String>) + 'static,
) -> Result<&'static EventSource, Error> {
    require(&endpoint.route, query, advertised.info())?;
    let source = event_source::new(&endpoint.url(query), None);
    source.add_event_listener(
        events::Message,
        move |event: &MessageEvent| match api::event(&js::string(event.data())) {
            Ok(Some(event)) => receive(source, Ok(event)),
            Ok(None) => {}
            Err(error) => receive(source, Err(error.to_string())),
        },
        None,
    );
    Ok(source)
}
