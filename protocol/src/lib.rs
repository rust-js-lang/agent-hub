#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! What the hub, the command line and the dashboard agree on: the hub's
//! routes, their JSON, and what an agent's name may be.
//!
//! The hub is native Rust, and so is the command line. The dashboard is this
//! same crate compiled to JavaScript by rust-js, so a change here is checked
//! against all three.

use serde::{Deserialize, Serialize};

pub mod endpoints;

/// `GET`: every agent the hub knows, as [`Agent`]s. `POST` a [`Register`]:
/// the agent is seen now, and the hub answers with its [`Agent`].
pub const PROTOCOL: u32 = 1;
pub const PROTOCOL_HEADER: &str = "agent-hub-protocol";
pub const EVENT_TYPES: &str = "sent,read,agent,retired,restarting";
pub const WRITE_NONCE_HEADER: &str = "agent-hub-nonce";
pub const ACK: &str = "/api/ack";
pub const BINARY: &str = "/api/binary";

pub const AGENTS: &str = "/api/agents";

/// `GET`: the history, newest first, as [`Message`]s. `POST` a [`Send`]: the
/// hub answers with the [`Message`] it stored.
pub const MESSAGES: &str = "/api/messages";

/// `POST` a [`ReadInbox`]: the agent's unread messages, oldest first, as an
/// [`Inbox`]. They're read from then on.
pub const INBOX: &str = "/api/inbox";
/// `GET`: actionable unread counts by sender; never acknowledges mail.
pub const NOTICE: &str = "/api/inbox/notice";
pub const READ: &str = "/api/messages/read";
pub const RETIRE: &str = "/api/agents/retire";

/// `GET`: server-sent events, each an [`Event`]'s JSON. With `?agent=<name>`,
/// only the messages sent to that agent, and the agent is watching while
/// the stream is open.
pub const EVENTS: &str = "/api/events";

/// `GET`: every hold held now, by name, as [`Hold`]s. `POST` a [`Take`]: the
/// hub answers with a [`Taken`], the hold as it is now and whether it's the
/// asker's.
pub const HOLDS: &str = "/api/holds";

/// `POST` a [`Release`]: its holder lets a hold go. The hub answers with the
/// [`Hold`]s still held.
pub const RELEASE: &str = "/api/holds/release";

/// `GET`: every channel, as [`Channel`]s, `everyone` first. `POST` a
/// [`Join`]: the agent is in the channel from then on. The hub answers
/// with the [`Channel`].
pub const CHANNELS: &str = "/api/channels";

/// `POST` a [`Join`]: the agent leaves the channel. The hub answers with
/// the [`Channel`] as it's left.
pub const LEAVE: &str = "/api/channels/leave";

/// `POST` a [`Send`] whose `to` is a channel's name: a copy goes to each
/// agent in it but its sender. The hub answers with the [`Message`]s it
/// stored, one a recipient.
pub const BROADCAST: &str = "/api/channels/messages";

/// The channel every agent is in, with no joining.
pub const EVERYONE: &str = "everyone";

/// `POST` a [`SignIn`]: the person the agents work for signs in from the
/// dashboard, with the hub's key. The hub answers with their [`Agent`].
pub const PERSON: &str = "/api/person";

/// `GET`: the hub itself, as a [`HubInfo`]: which build is running.
pub const HUB: &str = "/api/hub";

/// `POST`: the hub stops, so another, a newer build's, can take its place
/// (`serve --replace`). It answers with its [`HubInfo`] first.
pub const STOP: &str = "/api/hub/stop";

/// The most messages `GET` [`MESSAGES`] gives.
pub const HISTORY_LIMIT: u32 = 500;

/// How long a hold lasts where its taker doesn't say, in minutes.
pub const HOLD_MINUTES: u32 = 30;

/// The longest a hold may be taken for at once, in minutes. Taking it again
/// keeps it longer.
pub const HOLD_MINUTES_MAX: u32 = 240;

/// An agent the hub knows: one that ran a command, or is watching.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Agent {
    pub name: String,
    /// The machine it last ran a command on.
    pub machine: String,
    /// When it last ran a command, in milliseconds since the Unix epoch.
    pub last_seen: u64,
    /// Whether a `watch` of it is connected now.
    pub watching: bool,
    /// Messages sent to it that it hasn't read.
    pub unread: u32,
    /// Whether it's the person the agents work for, signed in from the
    /// dashboard with the hub's key: only the dashboard sends as it.
    pub person: bool,
    #[serde(default)]
    pub away: bool,
    #[serde(default)]
    pub retired_at: Option<u64>,
}

/// A channel: a name, and the agents a message to it goes to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Channel {
    pub name: String,
    /// The agents in it, by name.
    pub members: Vec<String>,
}

/// An agent joins a channel, or leaves it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Join {
    pub channel: String,
    pub name: String,
}

/// The person signs in from the dashboard: the name they'll write as, and
/// the hub's key, which `agent-hub key` prints on its machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignIn {
    pub name: String,
    pub key: String,
}

/// An agent says who it is, and where it runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Register {
    pub name: String,
    pub machine: String,
    /// A reconnect must never revive a retired name.
    #[serde(default)]
    pub reconnect: bool,
}

/// A message to send.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Send {
    pub from: String,
    pub to: String,
    pub body: String,
    /// The hub's key, of a message the person sends from the dashboard;
    /// none of an agent's.
    pub key: Option<String>,
}

/// A message the hub stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: u64,
    pub from: String,
    pub to: String,
    pub body: String,
    /// In milliseconds since the Unix epoch.
    pub sent_at: u64,
    /// When its recipient read it, if it has.
    pub read_at: Option<u64>,
    /// Whether the person sent it, from the dashboard with the hub's key:
    /// no agent's command sends one that says so.
    pub person: bool,
    /// The channel it was sent to, of which this is its recipient's copy;
    /// none of one sent to its recipient alone.
    pub channel: Option<String>,
    #[serde(default)]
    pub away: bool,
    #[serde(default)]
    pub urgent: bool,
}

/// An agent reads its unread messages, at most `max` of them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadInbox {
    pub name: String,
    pub max: u32,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub sender: Option<String>,
    #[serde(default)]
    pub skip: bool,
    /// Opt in, so clients that cannot display digests still get full bodies.
    #[serde(default)]
    pub digest: bool,
    #[serde(default)]
    pub defer_ack: bool,
    #[serde(default)]
    pub up_to: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadMessage {
    pub name: String,
    pub id: u64,
    #[serde(default)]
    pub defer_ack: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retire {
    pub name: String,
    pub key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retired {
    pub channels: u32,
    pub holds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestLine {
    pub id: u64,
    pub from: String,
    pub first_line: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Digest {
    pub channel: String,
    pub unread: u32,
    pub first: Vec<DigestLine>,
    #[serde(default)]
    pub newest_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwaySummary {
    pub channel: String,
    pub count: u32,
    pub senders: Vec<(String, u32)>,
}

/// Unread messages, oldest first, and how many are still unread after them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Inbox {
    pub messages: Vec<Message>,
    pub remaining: u32,
    #[serde(default)]
    pub digests: Vec<Digest>,
    #[serde(default)]
    pub away: Vec<AwaySummary>,
    #[serde(default)]
    pub read_ids: Vec<u64>,
    #[serde(default)]
    pub skipped: u32,
}

/// The hub that's running: an agent's command of another build may not
/// know all it does, or it all the command asks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubInfo {
    /// Its release, `0.1.0`.
    pub version: String,
    /// The commit it was built from, `06dcee9`, or `unknown`.
    pub build: String,
    /// When it started, in milliseconds since the Unix epoch.
    pub started: u64,
    #[serde(default = "default_away_after")]
    pub away_after: u64,
    #[serde(default = "default_forget_after")]
    pub forget_after: u64,
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(default = "default_protocol")]
    pub protocol: u32,
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub binary_sha256: Option<String>,
    #[serde(default)]
    pub supervised: bool,
    #[serde(default)]
    pub addresses: Vec<ListenAddress>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AddressState {
    Ready,
    Pending,
    Blocked,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListenAddress {
    pub address: String,
    pub state: AddressState,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub remote_seen: bool,
}

pub fn default_protocol() -> u32 {
    PROTOCOL
}

pub fn default_away_after() -> u64 {
    60 * 60_000
}
pub fn default_forget_after() -> u64 {
    24 * 60 * 60_000
}

/// Something one agent has at a time, by its name: `check`, say, for a long
/// run that wants all of a machine's cores. It lapses at `until`, so an agent
/// that stopped without letting go doesn't keep it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hold {
    pub name: String,
    /// The agent that has it.
    pub holder: String,
    /// When its holder first took it, in milliseconds since the Unix epoch.
    pub since: u64,
    /// When it lapses unless taken again.
    pub until: u64,
}

/// An agent asks for a hold, for `minutes` from now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Take {
    pub name: String,
    pub holder: String,
    pub minutes: u32,
}

/// What a [`Take`] came to: the hold as it is now, the asker's where it was
/// free, lapsed or already its own, and another agent's otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Taken {
    pub hold: Hold,
    pub yours: bool,
}

/// An agent lets go of a hold it has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Release {
    pub name: String,
    pub holder: String,
}

/// Why the hub refused a request: the body of every answer that isn't 200.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refusal {
    #[serde(default)]
    pub code: Option<String>,
    pub reason: String,
}

/// What happened on the hub, as [`EVENTS`] streams it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Event {
    /// A message was sent.
    Sent {
        message: Message,
    },
    /// An agent read these messages.
    Read {
        name: String,
        ids: Vec<u64>,
    },
    /// An agent was seen, or started or stopped watching.
    Agent {
        agent: Agent,
    },
    Retired {
        name: String,
    },
    Restarting,
    #[serde(other)]
    Unknown,
}

impl Event {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Sent { .. } => "sent",
            Self::Read { .. } => "read",
            Self::Agent { .. } => "agent",
            Self::Retired { .. } => "retired",
            Self::Restarting => "restarting",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acknowledge {
    pub name: String,
    pub ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Notice {
    pub senders: Vec<(String, bool, u32)>,
    pub newest_unread_id: u64,
}

/// The longest name an agent may have.
pub const NAME_MAX: usize = 64;

/// Whether `name` may name an agent: 1 to [`NAME_MAX`] ASCII letters, digits,
/// `-`, `_` and `.`, so it's the same in a URL, a shell and a terminal.
pub fn check_name(name: &str) -> Result<(), String> {
    if name == "hub" {
        return Err("hub is reserved for upgrade announcements".to_string());
    }
    check(name, "an agent's name")
}

/// Whether `name` may name a channel: what an agent's name may be.
pub fn check_channel(name: &str) -> Result<(), String> {
    check(name, "a channel's name")
}

/// Whether `name` may name a hold: what an agent's name may be.
pub fn check_hold(name: &str) -> Result<(), String> {
    check(name, "a hold's name")
}

/// Whether `name` is a name, of `what` it names.
fn check(name: &str, what: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{what} can't be empty"));
    }
    if name.len() > NAME_MAX {
        return Err(format!("{what} has at most {NAME_MAX} characters: {name}"));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')))
    {
        return Err(format!(
            "{what} is letters, digits, `-`, `_` and `.`, and {name:?} has {bad:?}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_letters_digits_and_three_marks() {
        assert_eq!(check_name("m6-react_2.b"), Ok(()));
        assert!(check_name("").is_err());
        assert!(check_name("has space").is_err());
        assert!(check_name("a/b").is_err());
        assert!(check_name("é").is_err());
        assert_eq!(check_name(&"a".repeat(NAME_MAX)), Ok(()));
        assert!(check_name(&"a".repeat(NAME_MAX + 1)).is_err());
    }

    #[test]
    fn a_hold_is_named_as_an_agent_is() {
        assert_eq!(check_hold("check"), Ok(()));
        assert_eq!(
            check_hold(""),
            Err("a hold's name can't be empty".to_string())
        );
        assert_eq!(
            check_name(""),
            Err("an agent's name can't be empty".to_string())
        );
        assert!(check_hold("all cores").is_err());
    }

    #[test]
    fn json_is_camel_case_and_events_are_tagged() {
        let message = Message {
            id: 7,
            from: "a".to_string(),
            to: "b".to_string(),
            body: "hi".to_string(),
            sent_at: 1,
            read_at: None,
            person: false,
            channel: None,
            away: false,
            urgent: false,
        };
        let event = Event::Sent { message };
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(
            json,
            r#"{"type":"sent","message":{"id":7,"from":"a","to":"b","body":"hi","sentAt":1,"readAt":null,"person":false,"channel":null,"away":false,"urgent":false}}"#
        );
        assert_eq!(serde_json::from_str::<Event>(&json).unwrap(), event);
    }
    #[test]
    fn legacy_json_preserves_full_inbox_behavior_and_new_fields_default() {
        let read: ReadInbox = serde_json::from_str(r#"{"name":"a","max":20}"#).unwrap();
        assert!(!read.digest && !read.skip && read.channel.is_none() && read.sender.is_none());
        let register: Register = serde_json::from_str(r#"{"name":"a","machine":"m"}"#).unwrap();
        assert!(!register.reconnect);
        let inbox: Inbox = serde_json::from_str(r#"{"messages":[],"remaining":0}"#).unwrap();
        assert!(inbox.away.is_empty() && inbox.digests.is_empty() && inbox.read_ids.is_empty());
        let info: HubInfo =
            serde_json::from_str(r#"{"version":"old","build":"old","started":1}"#).unwrap();
        assert_eq!(info.away_after, default_away_after());
        assert!(info.features.is_empty());
        let agent: Agent = serde_json::from_str(
            r#"{"name":"a","machine":"m","lastSeen":1,"watching":false,"unread":0,"person":false}"#,
        )
        .unwrap();
        assert!(!agent.away && agent.retired_at.is_none());
        let message: Message = serde_json::from_str(r#"{"id":1,"from":"a","to":"b","body":"hi","sentAt":1,"readAt":null,"person":false,"channel":null}"#).unwrap();
        assert!(!message.away);
    }
    #[test]
    fn unknown_event_variants_and_extra_fields_are_forward_compatible() {
        assert_eq!(
            serde_json::from_str::<Event>(r#"{"type":"future-event","whatever":{"nested":true}}"#)
                .unwrap(),
            Event::Unknown
        );
        let event: Event =
            serde_json::from_str(r#"{"type":"retired","name":"a","future":123}"#).unwrap();
        assert_eq!(
            event,
            Event::Retired {
                name: "a".to_string()
            }
        );
        let info: HubInfo =
            serde_json::from_str(r#"{"version":"old","build":"old","started":1,"future":true}"#)
                .unwrap();
        assert_eq!(info.protocol, 1);
        let refusal: Refusal =
            serde_json::from_str(r#"{"reason":"old reason","future":true}"#).unwrap();
        assert!(refusal.code.is_none());
    }
    #[test]
    fn legacy_hubs_have_unknown_addresses_and_future_address_states_are_safe() {
        let old: HubInfo =
            serde_json::from_value(serde_json::json!({"version":"old","build":"old","started":1}))
                .unwrap();
        assert!(old.addresses.is_empty());
        let status: ListenAddress = serde_json::from_value(
            serde_json::json!({"address":"100.64.1.1:7878","state":"a-future-state","extra":true}),
        )
        .unwrap();
        assert_eq!(status.state, AddressState::Unknown);
        assert!(!status.remote_seen);
        assert!(status.reason.is_none());
    }

    #[test]
    fn protocol_objects_accept_additive_unknown_fields() {
        fn accepts<T: serde::de::DeserializeOwned>(mut value: serde_json::Value) {
            value.as_object_mut().unwrap().insert(
                "field_from_a_future_release".to_string(),
                serde_json::json!({"nested":true}),
            );
            serde_json::from_value::<T>(value).unwrap_or_else(|e| {
                panic!(
                    "{} rejected an extra field: {e}",
                    std::any::type_name::<T>()
                )
            });
        }
        accepts::<Agent>(
            serde_json::json!({"name":"a","machine":"m","lastSeen":1,"watching":false,"unread":0,"person":false}),
        );
        accepts::<Message>(
            serde_json::json!({"id":1,"from":"a","to":"b","body":"hi","sentAt":1,"readAt":null,"person":false,"channel":null}),
        );
        accepts::<Acknowledge>(serde_json::json!({"name":"a","ids":[]}));
        accepts::<Notice>(serde_json::json!({"senders":[],"newest_unread_id":0}));
        accepts::<Register>(serde_json::json!({"name":"a","machine":"m"}));
        accepts::<Send>(serde_json::json!({"from":"a","to":"b","body":"hi"}));
        accepts::<ReadInbox>(serde_json::json!({"name":"a","max":20}));
        accepts::<ReadMessage>(serde_json::json!({"name":"a","id":1}));
        accepts::<Retire>(serde_json::json!({"name":"a"}));
        accepts::<Retired>(serde_json::json!({"channels":0,"holds":0}));
        accepts::<Channel>(serde_json::json!({"name":"team","members":[]}));
        accepts::<Join>(serde_json::json!({"name":"a","channel":"team"}));
        accepts::<SignIn>(serde_json::json!({"name":"person","key":"key"}));
        accepts::<Take>(serde_json::json!({"name":"check","holder":"a","minutes":30}));
        accepts::<Release>(serde_json::json!({"name":"check","holder":"a"}));
        accepts::<Hold>(serde_json::json!({"name":"check","holder":"a","since":1,"until":2}));
        accepts::<Taken>(
            serde_json::json!({"hold":{"name":"check","holder":"a","since":1,"until":2},"yours":true}),
        );
        accepts::<Inbox>(serde_json::json!({"messages":[],"remaining":0}));
        accepts::<HubInfo>(serde_json::json!({"version":"old","build":"old","started":1}));
        accepts::<Refusal>(serde_json::json!({"reason":"reason"}));
        accepts::<Digest>(serde_json::json!({"channel":"team","unread":0,"first":[]}));
        accepts::<DigestLine>(serde_json::json!({"id":1,"from":"a","first_line":"ask"}));
        accepts::<AwaySummary>(serde_json::json!({"channel":"team","count":0,"senders":[]}));
    }
}
