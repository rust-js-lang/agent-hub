//! The wire contract shared by the hub and its two transports.
//!
//! Query parsing receives decoded pairs: percent decoding belongs to the
//! HTTP transport, while defaults and validation belong here.
//!
//! An endpoint fixes both request and response types:
//! ```compile_fail
//! use protocol::endpoints::{Post, SEND};
//! let _: &Post<protocol::Register, protocol::Message> = &SEND;
//! ```
//! ```compile_fail
//! use protocol::endpoints::{Post, SEND};
//! let _: &Post<protocol::Send, protocol::Agent> = &SEND;
//! ```
//! ```compile_fail
//! use protocol::endpoints::{Get, SEND};
//! let _: &Get<protocol::Send, protocol::Message> = &SEND;
//! ```

use serde::de::DeserializeOwned;

use crate::{Event, Refusal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feature {
    InboxDigests,
    AgentLifecycle,
    BinaryDownload,
    ReadAck,
    BoundedSkip,
    NoticeId,
}

impl Feature {
    pub fn name(&self) -> &'static str {
        match self {
            Self::InboxDigests => "inbox-digests",
            Self::AgentLifecycle => "agent-lifecycle",
            Self::BinaryDownload => "binary-download",
            Self::ReadAck => "read-ack",
            Self::BoundedSkip => "bounded-skip",
            Self::NoticeId => "notice-id",
        }
    }
}

/// Additive fields that change an operation require feature negotiation.
/// A request may require several features at once.
pub trait Request {
    fn needs(&self) -> Vec<Feature> {
        Vec::new()
    }
}

macro_rules! plain_requests {
    ($($request:ty),* $(,)?) => { $(impl Request for $request {})* };
}

plain_requests!(
    (),
    History,
    Events,
    crate::Send,
    crate::Join,
    crate::SignIn,
    crate::Take,
    crate::Release,
    crate::Retire,
    crate::Acknowledge
);

impl Request for crate::ReadInbox {
    fn needs(&self) -> Vec<Feature> {
        let mut features = Vec::new();
        if self.digest || self.skip || self.channel.is_some() || self.sender.is_some() {
            features.push(Feature::InboxDigests);
        }
        if self.defer_ack {
            features.push(Feature::ReadAck);
        }
        if self.up_to.is_some() {
            features.push(Feature::BoundedSkip);
        }
        features
    }
}

impl Request for crate::ReadMessage {
    fn needs(&self) -> Vec<Feature> {
        if self.defer_ack {
            vec![Feature::ReadAck]
        } else {
            Vec::new()
        }
    }
}

impl Request for crate::Register {
    fn needs(&self) -> Vec<Feature> {
        if self.reconnect {
            vec![Feature::AgentLifecycle]
        } else {
            Vec::new()
        }
    }
}

impl Request for NoticeQuery {
    fn needs(&self) -> Vec<Feature> {
        if self.with_id {
            vec![Feature::NoticeId]
        } else {
            Vec::new()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Json,
    Stream,
    Download,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Route {
    pub name: &'static str,
    pub method: Method,
    pub path: &'static str,
    pub kind: Kind,
    pub needs: Option<Feature>,
}

macro_rules! endpoint_kind {
    ($kind:ident, $method:ident, $wire:ident) => {
        pub struct $kind<Req, Res> {
            pub route: Route,
            types: std::marker::PhantomData<fn(Req) -> Res>,
        }

        impl<Req, Res> $kind<Req, Res> {
            const fn new(name: &'static str, path: &'static str, needs: Option<Feature>) -> Self {
                Self {
                    route: Route {
                        name,
                        method: Method::$method,
                        path,
                        kind: Kind::$wire,
                        needs,
                    },
                    types: std::marker::PhantomData,
                }
            }
        }
    };
}

endpoint_kind!(Get, Get, Json);
endpoint_kind!(Post, Post, Json);
endpoint_kind!(Stream, Get, Stream);
endpoint_kind!(Download, Get, Download);

macro_rules! query_url {
    ($($kind:ident),*) => { $(
        impl<Q: Query, Res> $kind<Q, Res> {
            pub fn url(&self, query: &Q) -> String { url(self.route.path, query) }
        }
    )* };
}
query_url!(Get, Stream, Download);

impl<B, Res> Post<B, Res> {
    pub fn url(&self) -> &'static str {
        self.route.path
    }
}

/// This endpoint has two historical response forms, selected by `with_id`.
/// Keep both typed and keep the old wire representation unchanged.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum NoticeResponse {
    Snapshot(crate::Notice),
    Legacy(Vec<(String, bool, u32)>),
}

macro_rules! endpoints {
    ($($name:ident: $kind:ident<$req:ty, $res:ty> = $path:expr, $needs:expr;)*) => {
        $(pub const $name: $kind<$req, $res> = $kind::new(stringify!($name), $path, $needs);)*
        pub const ALL: &[Route] = &[$($name.route,)*];
    };
}

endpoints! {
    AGENTS: Get<(), Vec<crate::Agent>> = crate::AGENTS, None;
    REGISTER: Post<crate::Register, crate::Agent> = crate::AGENTS, None;
    HISTORY: Get<History, Vec<crate::Message>> = crate::MESSAGES, None;
    SEND: Post<crate::Send, crate::Message> = crate::MESSAGES, None;
    INBOX: Post<crate::ReadInbox, crate::Inbox> = crate::INBOX, None;
    ACK: Post<crate::Acknowledge, Vec<u64>> = crate::ACK, Some(Feature::ReadAck);
    READ: Post<crate::ReadMessage, crate::Message> = crate::READ, Some(Feature::InboxDigests);
    RETIRE: Post<crate::Retire, crate::Retired> = crate::RETIRE, Some(Feature::AgentLifecycle);
    NOTICE: Get<NoticeQuery, NoticeResponse> = crate::NOTICE, Some(Feature::InboxDigests);
    HEALTH: Get<(), String> = "/api/health", None;
    BINARY: Download<(), Vec<u8>> = crate::BINARY, Some(Feature::BinaryDownload);
    HOLDS: Get<(), Vec<crate::Hold>> = crate::HOLDS, None;
    TAKE: Post<crate::Take, crate::Taken> = crate::HOLDS, None;
    RELEASE: Post<crate::Release, Vec<crate::Hold>> = crate::RELEASE, None;
    CHANNELS: Get<(), Vec<crate::Channel>> = crate::CHANNELS, None;
    JOIN: Post<crate::Join, crate::Channel> = crate::CHANNELS, None;
    LEAVE: Post<crate::Join, crate::Channel> = crate::LEAVE, None;
    BROADCAST: Post<crate::Send, Vec<crate::Message>> = crate::BROADCAST, None;
    PERSON: Post<crate::SignIn, crate::Agent> = crate::PERSON, None;
    HUB: Get<(), crate::HubInfo> = crate::HUB, None;
    STOP: Post<(), crate::HubInfo> = crate::STOP, None;
    EVENTS: Stream<Events, crate::Event> = crate::EVENTS, None;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    Retired,
    UpgradeRequired,
    NotFound,
    Other,
}

/// Keep the HTTP status even when an old hub has no refusal code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    pub status: u16,
    pub refusal: Refusal,
}

impl Refused {
    pub fn code(&self) -> Code {
        match self.refusal.code.as_deref() {
            Some("retired") => Code::Retired,
            Some("upgrade-required") => Code::UpgradeRequired,
            Some("not-found" | "unknown-agent") => Code::NotFound,
            Some(_) => Code::Other,
            None => match self.status {
                410 => Code::Retired,
                426 => Code::UpgradeRequired,
                404 => Code::NotFound,
                _ => Code::Other,
            },
        }
    }
}

/// A JSON endpoint succeeds with 200. Invalid success JSON is a decode
/// failure, never a successful response with invented default fields.
pub fn read<Res: DeserializeOwned>(status: u16, text: &str) -> Result<Res, Refused> {
    if status == 200 {
        return serde_json::from_str(text).map_err(|error| Refused {
            status,
            refusal: Refusal {
                code: None,
                reason: format!("The hub's answer isn't what this version reads: {error}"),
            },
        });
    }
    Err(Refused {
        status,
        refusal: serde_json::from_str(text).unwrap_or_else(|_| Refusal {
            code: None,
            reason: format!("The hub answered {status}."),
        }),
    })
}

/// The transport assembles SSE data lines before calling this decoder.
/// Unknown event types are additive; a broken known event is an error.
pub fn event(data: &str) -> Result<Option<Event>, serde_json::Error> {
    match serde_json::from_str(data)? {
        Event::Unknown => Ok(None),
        event => Ok(Some(event)),
    }
}

pub trait Query: Sized {
    fn pairs(&self) -> Vec<(&'static str, String)>;
    fn parse(pairs: &[(String, String)]) -> Result<Self, String>;
}

impl Query for () {
    fn pairs(&self) -> Vec<(&'static str, String)> {
        Vec::new()
    }

    fn parse(_: &[(String, String)]) -> Result<Self, String> {
        Ok(())
    }
}

fn value<'a>(pairs: &'a [(String, String)], key: &str) -> Result<Option<&'a str>, String> {
    let mut result = None;
    for (name, value) in pairs {
        if name == key {
            if result.is_some() {
                return Err(format!("duplicate query parameter: {key}"));
            }
            result = Some(value.as_str());
        }
    }
    Ok(result)
}

fn number(pairs: &[(String, String)], key: &str) -> Result<Option<u32>, String> {
    value(pairs, key)?
        .map(|text| text.parse().map_err(|_| format!("invalid {key}: {text}")))
        .transpose()
}

fn boolean(pairs: &[(String, String)], key: &str) -> Result<bool, String> {
    match value(pairs, key)? {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(text) => Err(format!("invalid {key}: {text}")),
    }
}

fn encode(text: &str) -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut encoded = String::new();
    for &byte in text.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 15) as usize] as char);
        }
    }
    encoded
}

pub fn url(path: &str, query: &impl Query) -> String {
    let pairs = query.pairs();
    let mut result = path.to_string();
    for (index, (key, value)) in pairs.iter().enumerate() {
        result.push(if index == 0 { '?' } else { '&' });
        result.push_str(&encode(key));
        result.push('=');
        result.push_str(&encode(value));
    }
    result
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct History {
    pub agent: Option<String>,
    pub limit: Option<u32>,
}

impl Query for History {
    fn pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        if let Some(agent) = &self.agent {
            pairs.push(("agent", agent.clone()));
        }
        if let Some(limit) = self.limit {
            pairs.push(("limit", limit.to_string()));
        }
        pairs
    }

    fn parse(pairs: &[(String, String)]) -> Result<Self, String> {
        Ok(Self {
            agent: value(pairs, "agent")?.map(str::to_string),
            limit: number(pairs, "limit")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Events {
    pub agent: Option<String>,
    pub protocol: Option<u32>,
    /// Omitted means the original three event types. Present and empty
    /// means no events. Unknown names are retained but never emitted.
    pub types: Option<Vec<String>>,
}

impl Events {
    pub fn accepts(&self, event: &Event) -> bool {
        match &self.types {
            Some(types) => types.iter().any(|kind| kind == event.kind()),
            None => matches!(
                event,
                Event::Sent { .. } | Event::Read { .. } | Event::Agent { .. }
            ),
        }
    }
}

impl Query for Events {
    fn pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        if let Some(agent) = &self.agent {
            pairs.push(("agent", agent.clone()));
        }
        if let Some(protocol) = self.protocol {
            pairs.push(("protocol", protocol.to_string()));
        }
        if let Some(types) = &self.types {
            pairs.push(("types", types.join(",")));
        }
        pairs
    }

    fn parse(pairs: &[(String, String)]) -> Result<Self, String> {
        Ok(Self {
            agent: value(pairs, "agent")?.map(str::to_string),
            protocol: number(pairs, "protocol")?,
            types: value(pairs, "types")?.map(|text| {
                text.split(',')
                    .filter(|name| !name.is_empty())
                    .map(str::to_string)
                    .collect()
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoticeQuery {
    pub name: String,
    pub channels: bool,
    pub with_id: bool,
}

impl Query for NoticeQuery {
    fn pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            ("name", self.name.clone()),
            ("channels", self.channels.to_string()),
            ("with_id", self.with_id.to_string()),
        ]
    }

    fn parse(pairs: &[(String, String)]) -> Result<Self, String> {
        Ok(Self {
            name: value(pairs, "name")?
                .ok_or_else(|| "missing name".to_string())?
                .to_string(),
            channels: boolean(pairs, "channels")?,
            with_id: boolean(pairs, "with_id")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_additive_inbox_behavior_is_gated_including_sender_alone() {
        let mut request: crate::ReadInbox =
            serde_json::from_str(r#"{"name":"a","max":10}"#).unwrap();
        assert!(request.needs().is_empty());
        request.sender = Some("b".to_string());
        assert_eq!(request.needs(), vec![Feature::InboxDigests]);
        request.defer_ack = true;
        request.up_to = Some(5);
        assert_eq!(
            request.needs(),
            vec![
                Feature::InboxDigests,
                Feature::ReadAck,
                Feature::BoundedSkip
            ]
        );
        request.sender = None;
        for field in ["digest", "skip", "channel"] {
            let json = match field {
                "channel" => r#"{"name":"a","max":10,"channel":"team"}"#,
                "skip" => r#"{"name":"a","max":10,"skip":true}"#,
                _ => r#"{"name":"a","max":10,"digest":true}"#,
            };
            let read: crate::ReadInbox = serde_json::from_str(json).unwrap();
            assert_eq!(read.needs(), vec![Feature::InboxDigests]);
        }
    }

    #[test]
    fn notice_wire_variants_are_both_typed_and_round_trip_unchanged() {
        for json in [
            r#"[["a",true,3]]"#,
            r#"{"senders":[["a",true,3]],"newest_unread_id":9}"#,
        ] {
            let response: NoticeResponse = read(200, json).unwrap();
            assert_eq!(serde_json::to_string(&response).unwrap(), json);
        }
        assert!(read::<NoticeResponse>(200, r#"{"senders":"wrong"}"#).is_err());
    }

    #[test]
    fn definition_table_has_unique_method_paths_and_distinct_transport_kinds() {
        for (index, route) in ALL.iter().enumerate() {
            assert!(
                !ALL[..index]
                    .iter()
                    .any(|previous| previous.method == route.method && previous.path == route.path)
            );
        }
        assert_eq!(BINARY.route.kind, Kind::Download);
        assert_eq!(EVENTS.route.kind, Kind::Stream);
        assert_eq!(SEND.route.kind, Kind::Json);
        assert_eq!(REGISTER.route.path, AGENTS.route.path);
        assert_ne!(REGISTER.route.method, AGENTS.route.method);
    }

    fn pairs(input: &[(&str, &str)]) -> Vec<(String, String)> {
        input
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn round_trip<Q: Query + std::fmt::Debug + PartialEq>(query: Q) {
        let encoded = query
            .pairs()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<Vec<_>>();
        assert_eq!(Q::parse(&encoded).unwrap(), query);
    }

    #[test]
    fn query_round_trips_preserve_empty_and_omitted_filters() {
        round_trip(History::default());
        round_trip(History {
            agent: Some("a + & é".to_string()),
            limit: Some(5),
        });
        round_trip(Events::default());
        round_trip(Events {
            agent: Some("a".to_string()),
            protocol: Some(1),
            types: Some(Vec::new()),
        });
        round_trip(Events {
            agent: None,
            protocol: Some(1),
            types: Some(vec!["read".to_string(), "future".to_string()]),
        });
        round_trip(NoticeQuery {
            name: "a".to_string(),
            channels: true,
            with_id: false,
        });
        assert!(!Events::default().accepts(&Event::Restarting));
        let empty = Events::parse(&pairs(&[("types", "")])).unwrap();
        assert!(!empty.accepts(&Event::Restarting));
        let future = Events::parse(&pairs(&[("types", "future,restarting")])).unwrap();
        assert!(future.accepts(&Event::Restarting));
    }

    #[test]
    fn urls_encode_utf8_and_delimiters_without_changing_values() {
        assert_eq!(url("/x", &()), "/x");
        assert_eq!(
            url(
                "/x",
                &History {
                    agent: Some("a +&é".to_string()),
                    limit: Some(2)
                }
            ),
            "/x?agent=a%20%2B%26%C3%A9&limit=2"
        );
        assert_eq!(
            url(
                "/x",
                &Events {
                    types: Some(vec!["sent".to_string(), "read".to_string()]),
                    ..Default::default()
                }
            ),
            "/x?types=sent%2Cread"
        );
    }

    #[test]
    fn queries_ignore_unknown_keys_but_reject_ambiguous_or_invalid_known_keys() {
        assert_eq!(
            History::parse(&pairs(&[("future", "anything")])).unwrap(),
            History::default()
        );
        assert!(History::parse(&pairs(&[("limit", "1"), ("limit", "2")])).is_err());
        assert!(History::parse(&pairs(&[("limit", "-1")])).is_err());
        assert!(Events::parse(&pairs(&[("protocol", "bad")])).is_err());
        assert!(NoticeQuery::parse(&pairs(&[("name", "a"), ("channels", "yes")])).is_err());
        assert!(NoticeQuery::parse(&[]).is_err());
    }

    #[test]
    fn refusals_keep_status_and_only_fall_back_when_code_is_absent() {
        let legacy = read::<crate::Notice>(410, r#"{"reason":"gone"}"#).unwrap_err();
        assert_eq!(legacy.status, 410);
        assert_eq!(legacy.code(), Code::Retired);
        assert_eq!(
            read::<crate::Notice>(400, r#"{"reason":"gone","code":"retired"}"#)
                .unwrap_err()
                .code(),
            Code::Retired
        );
        assert_eq!(
            read::<crate::Notice>(410, r#"{"reason":"new","code":"future"}"#)
                .unwrap_err()
                .code(),
            Code::Other
        );
        let invalid = read::<crate::Notice>(500, "not JSON").unwrap_err();
        assert_eq!(invalid.status, 500);
        assert!(invalid.refusal.reason.contains("500"));
        let notice =
            read::<crate::Notice>(200, r#"{"senders":[],"newest_unread_id":3,"future":true}"#)
                .unwrap();
        assert_eq!(notice.newest_unread_id, 3);
        assert!(read::<crate::Notice>(200, "{}").is_err());
    }

    #[test]
    fn unknown_events_are_skipped_and_corrupt_known_events_are_reported() {
        assert_eq!(event(r#"{"type":"future","extra":true}"#).unwrap(), None);
        assert_eq!(
            event("{\n\"type\":\"restarting\"\n}").unwrap(),
            Some(Event::Restarting)
        );
        assert!(event("broken").is_err());
        assert!(event(r#"{"type":"sent"}"#).is_err());
    }
}
