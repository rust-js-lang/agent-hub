//! What the hub, the command line and the dashboard agree on: the hub's
//! routes, their JSON, and what an agent's name may be.
//!
//! The hub is native Rust, and so is the command line. The dashboard is this
//! same crate compiled to JavaScript by rust-js, so a change here is checked
//! against all three.

use serde::{Deserialize, Serialize};

/// `GET`: every agent the hub knows, as [`Agent`]s. `POST` a [`Register`]:
/// the agent is seen now, and the hub answers with its [`Agent`].
pub const AGENTS: &str = "/api/agents";

/// `GET`: the history, newest first, as [`Message`]s. `POST` a [`Send`]: the
/// hub answers with the [`Message`] it stored.
pub const MESSAGES: &str = "/api/messages";

/// `POST` a [`ReadInbox`]: the agent's unread messages, oldest first, as an
/// [`Inbox`]. They're read from then on.
pub const INBOX: &str = "/api/inbox";

/// `GET`: server-sent events, each an [`Event`]'s JSON. With `?agent=<name>`,
/// only the messages sent to that agent, and the agent is watching while
/// the stream is open.
pub const EVENTS: &str = "/api/events";

/// The most messages `GET` [`MESSAGES`] gives.
pub const HISTORY_LIMIT: u32 = 500;

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
}

/// An agent says who it is, and where it runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Register {
    pub name: String,
    pub machine: String,
}

/// A message to send.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Send {
    pub from: String,
    pub to: String,
    pub body: String,
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
}

/// An agent reads its unread messages, at most `max` of them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadInbox {
    pub name: String,
    pub max: u32,
}

/// Unread messages, oldest first, and how many are still unread after them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Inbox {
    pub messages: Vec<Message>,
    pub remaining: u32,
}

/// Why the hub refused a request: the body of every answer that isn't 200.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refusal {
    pub reason: String,
}

/// What happened on the hub, as [`EVENTS`] streams it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Event {
    /// A message was sent.
    Sent { message: Message },
    /// An agent read these messages.
    Read { name: String, ids: Vec<u64> },
    /// An agent was seen, or started or stopped watching.
    Agent { agent: Agent },
}

/// The longest name an agent may have.
pub const NAME_MAX: usize = 64;

/// Whether `name` may name an agent: 1 to [`NAME_MAX`] ASCII letters, digits,
/// `-`, `_` and `.`, so it's the same in a URL, a shell and a terminal.
pub fn check_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("an agent's name can't be empty".to_string());
    }
    if name.len() > NAME_MAX {
        return Err(format!(
            "an agent's name has at most {NAME_MAX} characters: {name}"
        ));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')))
    {
        return Err(format!(
            "an agent's name is letters, digits, `-`, `_` and `.`, and {name:?} has {bad:?}"
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
    fn json_is_camel_case_and_events_are_tagged() {
        let message = Message {
            id: 7,
            from: "a".to_string(),
            to: "b".to_string(),
            body: "hi".to_string(),
            sent_at: 1,
            read_at: None,
        };
        let event = Event::Sent { message };
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(
            json,
            r#"{"type":"sent","message":{"id":7,"from":"a","to":"b","body":"hi","sentAt":1,"readAt":null}}"#
        );
        assert_eq!(serde_json::from_str::<Event>(&json).unwrap(), event);
    }
}
