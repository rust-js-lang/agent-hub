//! The dashboard's calls to the hub, and how it shows a time.

use crate::client;
use js::date;
use protocol::endpoints;
use protocol::{Agent, Channel, Inbox, Message, ReadInbox, Send, SignIn};
use std::cell::Cell;
use webapi::window;

thread_local! {
    static RELOADING: Cell<bool> = const { Cell::new(false) };
}

/// Several failed requests in one render still request one reload. Compose
/// already stores each draft edit before it can start a request.
pub fn reload() {
    RELOADING.with(|reloading| {
        if !reloading.replace(true) {
            window.location().reload();
        }
    });
}

fn problem(error: client::Error) -> String {
    if error.upgrade_required() {
        reload();
    }
    error.message()
}

/// Every agent, every channel, and the history, newest first.
pub async fn load() -> Result<(Vec<Agent>, Vec<Channel>, Vec<Message>), String> {
    let agents = client::get(&endpoints::AGENTS, &())
        .await
        .map_err(problem)?;
    let channels = client::get(&endpoints::CHANNELS, &())
        .await
        .map_err(problem)?;
    let messages = client::get(&endpoints::HISTORY, &endpoints::History::default())
        .await
        .map_err(problem)?;
    Ok((agents, channels, messages))
}

/// The person the agents work for, as this device remembers them: the
/// name they write as, and the hub's key.
#[derive(Clone, PartialEq)]
pub struct Me {
    pub name: String,
    pub key: String,
}

const NAME: &str = "agent-hub.name";
const KEY: &str = "agent-hub.key";

/// Who signed in on this device before, if anyone: kept in the browser,
/// where it may keep data.
pub fn me() -> Option<Me> {
    let storage = window.local_storage().ok()?;
    Some(Me {
        name: storage.get_item(NAME)?,
        key: storage.get_item(KEY)?,
    })
}

/// Signs the person in as `name` with the hub's `key`, and remembers them
/// on this device.
pub async fn sign_in(name: &str, key: &str) -> Result<Me, String> {
    let sign_in = SignIn {
        name: name.to_string(),
        key: key.to_string(),
    };
    let agent = client::post(&endpoints::PERSON, &sign_in)
        .await
        .map_err(problem)?;
    let me = Me {
        name: agent.name,
        key: key.to_string(),
    };
    if let Ok(storage) = window.local_storage() {
        let _ = storage.set_item(NAME, &me.name);
        let _ = storage.set_item(KEY, &me.key);
    }
    Ok(me)
}

/// Forgets who signed in on this device.
pub fn sign_out() {
    if let Ok(storage) = window.local_storage() {
        storage.remove_item(NAME);
        storage.remove_item(KEY);
    }
}

/// Sends `body` as the person to `to`: an agent, or a channel, `#team`,
/// each agent in which is sent a copy.
pub async fn send(me: &Me, to: &str, body: &str) -> Result<(), String> {
    let send = Send {
        from: me.name.clone(),
        to: to.to_string(),
        body: body.to_string(),
        key: Some(me.key.clone()),
    };
    if to.starts_with('#') {
        client::post(&endpoints::BROADCAST, &send)
            .await
            .map(|_| ())
            .map_err(problem)
    } else {
        client::post(&endpoints::SEND, &send)
            .await
            .map(|_| ())
            .map_err(problem)
    }
}

/// The person has read what was sent to them: it's on the page.
pub async fn read(me: &Me) -> Result<Inbox, String> {
    let read = ReadInbox {
        name: me.name.clone(),
        max: 100,
        channel: None,
        sender: None,
        skip: false,
        digest: false,

        defer_ack: false,

        up_to: None,
    };
    client::post(&endpoints::INBOX, &read)
        .await
        .map_err(problem)
}

pub async fn retire(me: &Me, name: &str) -> Result<(), String> {
    client::post(
        &endpoints::RETIRE,
        &protocol::Retire {
            name: name.to_string(),
            key: Some(me.key.clone()),
        },
    )
    .await
    .map_err(problem)?;
    Ok(())
}

/// A time in milliseconds since the Unix epoch, in the reader's locale.
pub fn time(at: u64) -> String {
    date::new(Some(&(at as f64))).to_locale_string(None, None)
}

pub async fn hub() -> Result<protocol::HubInfo, client::Error> {
    client::get(&endpoints::HUB, &()).await
}

const DRAFT: &str = "agent-hub.draft";
pub fn draft() -> String {
    window
        .local_storage()
        .ok()
        .and_then(|s| s.get_item(DRAFT))
        .unwrap_or_default()
}
pub fn set_draft(body: &str) {
    if let Ok(storage) = window.local_storage() {
        let _ = storage.set_item(DRAFT, body);
    }
}
