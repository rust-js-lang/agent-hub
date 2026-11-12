//! The dashboard's calls to the hub, and how it shows a time.

use js::{date, settle};
use protocol::{
    AGENTS, Agent, BROADCAST, CHANNELS, Channel, INBOX, Inbox, MESSAGES, Message, PERSON,
    ReadInbox, Refusal, Send, SignIn,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use webapi::{BodyInit, RequestInit, headers, window};

/// The JSON the hub answers at `path`, asked as `init` says, or why
/// there's none.
async fn ask<T: DeserializeOwned>(path: &str, init: Option<RequestInit<'_>>) -> Result<T, String> {
    let response = match settle(window.fetch(path, init)).await {
        Ok(response) => response,
        Err(_) => {
            return Err(
                "The hub can't be reached: is `cross-agent-message serve` running?".to_string(),
            );
        }
    };
    let status = response.status() as u16;
    let text = response.text().await;
    if status != 200 {
        return Err(match serde_json::from_str::<Refusal>(&text) {
            Ok(refusal) => refusal.reason,
            Err(_) => format!("The hub answered {status}."),
        });
    }
    serde_json::from_str(&text)
        .map_err(|error| format!("The hub's answer isn't what this page reads: {error}"))
}

/// The JSON the hub answers a `POST` of `body` to `path`.
async fn post<T: DeserializeOwned>(path: &str, body: &impl Serialize) -> Result<T, String> {
    let json = serde_json::to_string(body)
        .map_err(|error| format!("This page can't write what it sends: {error}"))?;
    let headers = headers::new(None);
    headers.append("content-type", "application/json");
    let init = RequestInit {
        method: Some("POST"),
        headers: Some(headers),
        body: Some(Some(BodyInit::Str(&json))),
        ..Default::default()
    };
    ask(path, Some(init)).await
}

/// Every agent, every channel, and the history, newest first.
pub async fn load() -> Result<(Vec<Agent>, Vec<Channel>, Vec<Message>), String> {
    let agents = ask(AGENTS, None).await?;
    let channels = ask(CHANNELS, None).await?;
    let messages = ask(MESSAGES, None).await?;
    Ok((agents, channels, messages))
}

/// The person the agents work for, as this device remembers them: the
/// name they write as, and the hub's key.
#[derive(Clone, PartialEq)]
pub struct Me {
    pub name: String,
    pub key: String,
}

const NAME: &str = "cross-agent-message.name";
const KEY: &str = "cross-agent-message.key";

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
    let agent: Agent = post(PERSON, &sign_in).await?;
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
        let _: Vec<Message> = post(BROADCAST, &send).await?;
    } else {
        let _: Message = post(MESSAGES, &send).await?;
    }
    Ok(())
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
    };
    post(INBOX, &read).await
}

pub async fn retire(me: &Me, name: &str) -> Result<(), String> {
    let _: protocol::Retired = post(
        protocol::RETIRE,
        &protocol::Retire {
            name: name.to_string(),
            key: Some(me.key.clone()),
        },
    )
    .await?;
    Ok(())
}

/// A time in milliseconds since the Unix epoch, in the reader's locale.
pub fn time(at: u64) -> String {
    date::new(Some(&(at as f64))).to_locale_string(None, None)
}
