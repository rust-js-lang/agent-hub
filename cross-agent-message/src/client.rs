//! A hub's API, from the command line.

use std::io::{BufRead, BufReader};

use anyhow::{Context, Result, bail};
use protocol::{
    AGENTS, Agent, EVENTS, Event, HOLDS, Hold, INBOX, Inbox, MESSAGES, Message, RELEASE, ReadInbox,
    Refusal, Register, Release, Send, Take, Taken,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use ureq::Body;
use ureq::http::Response;

pub struct Client {
    hub: String,
    http: ureq::Agent,
}

impl Client {
    /// A client of the hub at `hub`, such as `http://127.0.0.1:7878`.
    pub fn new(hub: &str) -> Client {
        let http = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        Client {
            hub: hub.trim_end_matches('/').to_string(),
            http,
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
        if response.status().is_success() {
            return response
                .body_mut()
                .read_json()
                .context("the hub's answer isn't what this version reads");
        }
        let status = response.status();
        match response.body_mut().read_json::<Refusal>() {
            Ok(refusal) => bail!("{}", refusal.reason),
            Err(_) => bail!("the hub answered {status}"),
        }
    }

    fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .http
            .get(self.url(path))
            .call()
            .with_context(|| self.unreachable())?;
        Client::answer(response)
    }

    fn post<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize) -> Result<T> {
        let response = self
            .http
            .post(self.url(path))
            .send_json(body)
            .with_context(|| self.unreachable())?;
        Client::answer(response)
    }

    /// `name` is seen now, on this machine.
    pub fn register(&self, name: &str) -> Result<Agent> {
        let machine = gethostname::gethostname()
            .to_string_lossy()
            .trim_end_matches(".local")
            .to_string();
        self.post(
            AGENTS,
            &Register {
                name: name.to_string(),
                machine,
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
            },
        )
    }

    pub fn read_inbox(&self, name: &str, max: u32) -> Result<Inbox> {
        self.post(
            INBOX,
            &ReadInbox {
                name: name.to_string(),
                max,
            },
        )
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
        let response = self
            .http
            .get(self.url(EVENTS))
            .query("agent", name)
            .call()
            .with_context(|| self.unreachable())?;
        if !response.status().is_success() {
            return Err(Client::answer::<Refusal>(response).unwrap_err());
        }
        let lines = BufReader::new(response.into_body().into_reader()).lines();
        // Server-sent events: each is a `data:` line; others keep it alive.
        Ok(lines.filter_map(|line| match line {
            Ok(line) => line.strip_prefix("data:").map(|json| {
                serde_json::from_str(json.trim_start())
                    .context("an event this version doesn't read")
            }),
            Err(error) => Some(Err(error).context("the hub closed the stream")),
        }))
    }
}
