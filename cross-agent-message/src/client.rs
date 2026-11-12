//! A hub's API, from the command line.

use std::io::{BufRead, BufReader};

use anyhow::{Context, Result, bail};
use protocol::{
    AGENTS, Agent, BROADCAST, CHANNELS, Channel, EVENTS, Event, HOLDS, HUB, Hold, HubInfo, INBOX,
    Inbox, Join, LEAVE, MESSAGES, Message, PERSON, READ, RELEASE, RETIRE, ReadInbox, ReadMessage,
    Refusal, Register, Release, Retire, Retired, STOP, Send, SignIn, Take, Taken,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use ureq::Body;
use ureq::http::Response;

#[derive(Debug)]
pub struct RetiredAgent(pub String);
impl std::fmt::Display for RetiredAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetiredAgent {}

pub struct Client {
    hub: String,
    http: ureq::Agent,
    metadata: std::sync::OnceLock<HubInfo>,
}

impl Client {
    /// A client of the hub at `hub`, such as `http://127.0.0.1:7878`.
    pub fn new(hub: &str) -> Client {
        let http = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_idle_connections(0)
            .timeout_global(Some(std::time::Duration::from_secs(15)))
            .build()
            .into();
        Client {
            hub: hub.trim_end_matches('/').to_string(),
            http,
            metadata: std::sync::OnceLock::new(),
        }
    }

    /// The hub's address.
    pub fn hub(&self) -> &str {
        &self.hub
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.hub)
    }

    fn unreachable(&self) -> String {
        format!(
            "can't reach the hub at {}: start it with `cross-agent-message serve`",
            self.hub
        )
    }

    /// The JSON of a 200 answer, or the hub's reason for refusing.
    fn answer<T: DeserializeOwned>(mut response: Response<Body>) -> Result<T> {
        if let Some(header) = response.headers().get(protocol::PROTOCOL_HEADER)
            && header.to_str().ok() != Some("1")
        {
            bail!("hub protocol is unsupported; upgrade the hub or client before sending commands");
        }
        if response.status().is_success() {
            return response
                .body_mut()
                .read_json()
                .context("the hub's answer isn't what this version reads");
        }
        let status = response.status();
        match response.body_mut().read_json::<Refusal>() {
            Ok(refusal) if status == ureq::http::StatusCode::GONE => {
                Err(anyhow::Error::new(RetiredAgent(refusal.reason)))
            }
            Ok(refusal) => bail!("{}", refusal.reason),
            Err(_) => bail!("the hub answered {status}"),
        }
    }

    fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .http
            .get(self.url(path))
            .header(protocol::PROTOCOL_HEADER, "1")
            .call()
            .with_context(|| self.unreachable())?;
        Client::answer(response)
    }

    fn post<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize) -> Result<T> {
        static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nonce = format!("{}-{}-{sequence}", std::process::id(), crate::hub::now());
        let value = serde_json::to_value(body)?;
        let safe = match path {
            AGENTS | CHANNELS | LEAVE | PERSON | protocol::ACK => true,
            MESSAGES | BROADCAST | HOLDS | RELEASE => self.has_feature("write-nonce")?,
            INBOX => {
                if value.get("skip").and_then(|v| v.as_bool()) == Some(true) {
                    value.get("up_to").is_some_and(|v| !v.is_null())
                        && self.has_feature("bounded-skip")?
                } else {
                    value.get("defer_ack").and_then(|v| v.as_bool()) == Some(true)
                        && self.has_feature("read-ack")?
                }
            }
            READ => value.get("defer_ack").and_then(|v| v.as_bool()) == Some(true),
            RETIRE => true,
            _ => false,
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let attempts = if safe { 3 } else { 1 };
        for attempt in 0..attempts {
            let response = self
                .http
                .post(self.url(path))
                .header(protocol::PROTOCOL_HEADER, "1")
                .header(protocol::WRITE_NONCE_HEADER, &nonce)
                .config()
                .timeout_global(Some(
                    deadline.saturating_duration_since(std::time::Instant::now()),
                ))
                .build()
                .send_json(&value);
            match response {
                Ok(response) => return Client::answer(response),
                Err(error) if attempt + 1 == attempts || std::time::Instant::now() >= deadline => {
                    return Err(error).with_context(|| self.unreachable());
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(100)),
            }
        }
        bail!("request retry budget exhausted")
    }

    pub fn has_feature(&self, feature: &str) -> Result<bool> {
        if self.metadata.get().is_none() {
            let info = self.check_protocol()?;
            let _ = self.metadata.set(info);
        }
        Ok(self
            .metadata
            .get()
            .is_some_and(|info| info.features.iter().any(|f| f == feature)))
    }

    /// `name` is seen now, on this machine.
    pub fn register(&self, name: &str) -> Result<Agent> {
        self.register_connection(name, false)
    }

    pub fn register_connection(&self, name: &str, reconnect: bool) -> Result<Agent> {
        let machine = gethostname::gethostname()
            .to_string_lossy()
            .trim_end_matches(".local")
            .to_string();
        self.post(
            AGENTS,
            &Register {
                name: name.to_string(),
                machine,
                reconnect,
            },
        )
    }

    pub fn agents(&self) -> Result<Vec<Agent>> {
        self.get(AGENTS)
    }

    pub fn send(&self, from: &str, to: &str, body: &str) -> Result<Message> {
        self.post(
            MESSAGES,
            &Send {
                from: from.to_string(),
                to: to.to_string(),
                body: body.to_string(),
                key: None,
            },
        )
    }

    /// Every channel, `everyone` first.
    pub fn channels(&self) -> Result<Vec<Channel>> {
        self.get(CHANNELS)
    }

    /// `name` joins `channel`: the channel, with it in.
    pub fn join(&self, channel: &str, name: &str) -> Result<Channel> {
        self.post(
            CHANNELS,
            &Join {
                channel: channel.to_string(),
                name: name.to_string(),
            },
        )
    }

    /// `name` leaves `channel`: the channel as it's left.
    pub fn leave(&self, channel: &str, name: &str) -> Result<Channel> {
        self.post(
            LEAVE,
            &Join {
                channel: channel.to_string(),
                name: name.to_string(),
            },
        )
    }

    /// `from`'s message to every other agent in `channel`: each one's copy.
    /// With the hub's `key`, the person's, as the dashboard sends it.
    pub fn broadcast(
        &self,
        from: &str,
        channel: &str,
        body: &str,
        key: Option<&str>,
    ) -> Result<Vec<Message>> {
        self.post(
            BROADCAST,
            &Send {
                from: from.to_string(),
                to: channel.to_string(),
                body: body.to_string(),
                key: key.map(str::to_string),
            },
        )
    }

    /// The person signs in as `name` with the hub's `key`, as the
    /// dashboard does: their agent.
    pub fn sign_in(&self, name: &str, key: &str) -> Result<Agent> {
        self.post(
            PERSON,
            &SignIn {
                name: name.to_string(),
                key: key.to_string(),
            },
        )
    }

    /// The person's message, sent with the hub's `key`, as the dashboard
    /// sends it.
    pub fn send_as_person(&self, from: &str, to: &str, body: &str, key: &str) -> Result<Message> {
        self.post(
            MESSAGES,
            &Send {
                from: from.to_string(),
                to: to.to_string(),
                body: body.to_string(),
                key: Some(key.to_string()),
            },
        )
    }

    pub fn read_inbox(&self, name: &str, max: u32) -> Result<Inbox> {
        self.post(
            INBOX,
            &ReadInbox {
                name: name.to_string(),
                max,
                channel: None,
                sender: None,
                skip: false,
                digest: false,

                defer_ack: false,

                up_to: None,
            },
        )
    }

    pub fn read_mail(&self, read: &ReadInbox) -> Result<Inbox> {
        self.post(INBOX, read)
    }

    pub fn read_message(&self, name: &str, id: u64) -> Result<Message> {
        self.require("inbox-digests")?;
        self.post(
            READ,
            &ReadMessage {
                name: name.to_string(),
                id,

                defer_ack: false,
            },
        )
    }

    pub fn retire(&self, name: &str) -> Result<Retired> {
        self.require("agent-lifecycle")?;
        self.post(
            RETIRE,
            &Retire {
                name: name.to_string(),
                key: None,
            },
        )
    }

    pub fn notice(&self, name: &str, channels: bool) -> Result<Vec<(String, bool, u32)>> {
        self.get(&format!(
            "/api/inbox/notice?name={name}&channels={channels}"
        ))
    }

    pub fn read_message_deferred(&self, name: &str, id: u64, defer_ack: bool) -> Result<Message> {
        self.require("inbox-digests")?;
        self.post(
            READ,
            &ReadMessage {
                name: name.to_string(),
                id,
                defer_ack,
            },
        )
    }
    pub fn acknowledge(&self, name: &str, ids: &[u64]) -> Result<Vec<u64>> {
        self.post(
            protocol::ACK,
            &protocol::Acknowledge {
                name: name.to_string(),
                ids: ids.to_vec(),
            },
        )
    }
    pub fn notice_snapshot(&self, name: &str, channels: bool) -> Result<protocol::Notice> {
        self.get(&format!(
            "{}?name={name}&channels={channels}&with_id=true",
            protocol::NOTICE
        ))
    }

    /// The hub that's running: its build.
    pub fn info(&self) -> Result<HubInfo> {
        self.get(HUB)
    }

    pub fn check_protocol(&self) -> Result<HubInfo> {
        let info = self.info()?;
        if info.protocol != protocol::PROTOCOL {
            bail!(
                "The hub at {} is {} (protocol {}); this command line needs protocol {}. Upgrade the hub on its machine: cross-agent-message upgrade <release>.",
                self.hub,
                info.version,
                info.protocol,
                protocol::PROTOCOL
            );
        }
        let _ = self.metadata.set(info.clone());
        Ok(info)
    }

    pub fn require(&self, feature: &str) -> Result<()> {
        let info = self.check_protocol()?;
        if !info.features.iter().any(|f| f == feature) {
            bail!(
                "This hub ({}) has no {feature} yet: upgrade it on its machine.",
                info.version
            );
        }
        Ok(())
    }

    pub fn download_binary(&self) -> Result<Vec<u8>> {
        let mut response = self
            .http
            .get(self.url(protocol::BINARY))
            .header(protocol::PROTOCOL_HEADER, "1")
            .call()?;
        if !response.status().is_success() {
            bail!("hub executable download failed: {}", response.status());
        }
        Ok(response
            .body_mut()
            .with_config()
            .limit(128 * 1024 * 1024)
            .read_to_vec()?)
    }

    /// Asks the hub to stop, for another to take its place: what it was.
    pub fn stop(&self) -> Result<HubInfo> {
        self.post(STOP, &())
    }

    /// What's held now, by name.
    pub fn holds(&self) -> Result<Vec<Hold>> {
        self.get(HOLDS)
    }

    /// `holder` asks for the hold `name`, for `minutes`: the hold as it is
    /// now, and whether it's `holder`'s.
    pub fn take(&self, name: &str, holder: &str, minutes: u32) -> Result<Taken> {
        self.post(
            HOLDS,
            &Take {
                name: name.to_string(),
                holder: holder.to_string(),
                minutes,
            },
        )
    }

    /// `holder` lets go of the hold `name`: what's still held.
    pub fn release(&self, name: &str, holder: &str) -> Result<Vec<Hold>> {
        self.post(
            RELEASE,
            &Release {
                name: name.to_string(),
                holder: holder.to_string(),
            },
        )
    }

    /// The messages sent to `name` from now on, as they arrive. `name` is
    /// watching until the iterator is dropped.
    pub fn watch(&self, name: &str) -> Result<impl Iterator<Item = Result<Event>>> {
        let http: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_idle_connections(0)
            .build()
            .into();
        let response = http
            .get(self.url(EVENTS))
            .query("agent", name)
            .query("types", protocol::EVENT_TYPES)
            .header(protocol::PROTOCOL_HEADER, "1")
            .call()
            .with_context(|| self.unreachable())?;
        if !response.status().is_success() {
            Client::answer::<Refusal>(response)?;
            bail!("the hub refused the watch without an error reason");
        }
        let lines = BufReader::new(response.into_body().into_reader()).lines();
        // Server-sent events: each is a `data:` line; others keep it alive.
        Ok(lines.filter_map(|line| match line {
            Ok(line) => {
                let json = line.strip_prefix("data:")?;
                match serde_json::from_str::<Event>(json.trim_start()) {
                    Ok(Event::Unknown) => None,
                    result => Some(result.context("an event this version doesn't read")),
                }
            }
            Err(error) => Some(Err(error).context("the hub closed the stream")),
        }))
    }
}
